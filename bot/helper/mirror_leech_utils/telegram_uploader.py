from PIL import Image
from aioshutil import rmtree
from asyncio import sleep
from logging import getLogger
from natsort import natsorted
from os import walk, path as ospath
import json
from time import time
from re import match as re_match, sub as re_sub, search as re_search
from pyrogram.errors import FloodWait, RPCError, FloodPremiumWait, BadRequest
from pyrogram.types import (
    InputMediaVideo,
    InputMediaDocument,
    InputMediaPhoto,
)
from aiofiles.os import (
    remove,
    path as aiopath,
    rename,
)
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    RetryError,
)

from ... import intervals
from ...core.config_manager import Config
from ...core.telegram_manager import TgClient
from ..ext_utils.bot_utils import sync_to_async, cmd_exec
from ..ext_utils.files_utils import is_archive, get_base_name
from ..ext_utils.status_utils import get_readable_file_size
from ..telegram_helper.message_utils import delete_message
from ..ext_utils.media_utils import (
    get_media_info,
    get_document_type,
    get_video_thumbnail,
    get_audio_thumbnail,
    get_multiple_frames_thumbnail,
)

LOGGER = getLogger(__name__)


class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._path = path
        self._start_time = time()
        self._total_files = 0
        self._thumb = self._listener.thumb or f"thumbnails/{listener.user_id}.jpg"
        self._msgs_dict = {}
        self._corrupted = 0
        self._is_corrupted = False
        self._media_dict = {"videos": {}, "documents": {}}
        self._last_msg_in_group = False
        self._up_path = ""
        self._lprefix = ""
        self._media_group = False
        self._is_private = False
        self._sent_msg = None
        self._user_session = self._listener.user_transmission
        self._error = ""
        self._base_msg = None
        self._files_links = False

    async def _upload_progress(self, current, _):
        if self._listener.is_cancelled:
            if self._user_session:
                TgClient.user.stop_transmission()
            else:
                self._listener.client.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def _user_settings(self):
        self._media_group = self._listener.user_dict.get("MEDIA_GROUP", False) or (
            Config.MEDIA_GROUP
            if "MEDIA_GROUP" not in self._listener.user_dict
            else False
        )
        self._lprefix = self._listener.user_dict.get("LEECH_FILENAME_PREFIX") or (
            Config.LEECH_FILENAME_PREFIX
            if "LEECH_FILENAME_PREFIX" not in self._listener.user_dict
            else ""
        )
        self._files_links = self._listener.user_dict.get("FILES_LINKS", False) or (
            Config.FILES_LINKS
            if "FILES_LINKS" not in self._listener.user_dict
            else False
        )
        if self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None

    async def _msg_to_reply(self):
        if self._listener.up_dest:
            msg = (
                self._listener.message.link
                if self._listener.is_super_chat
                else self._listener.message.text.lstrip("/")
            )
            try:
                if self._user_session:
                    self._sent_msg = await TgClient.user.send_message(
                        chat_id=self._listener.up_dest,
                        text=msg,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                    )
                else:
                    self._sent_msg = await self._listener.client.send_message(
                        chat_id=self._listener.up_dest,
                        text=msg,
                        message_thread_id=self._listener.chat_thread_id,
                        disable_notification=True,
                    )
                    self._is_private = self._sent_msg.chat.type.name == "PRIVATE"
            except Exception as e:
                await self._listener.on_upload_error(str(e))
                return False
            finally:
                self._base_msg = self._sent_msg
        elif self._user_session:
            self._sent_msg = await TgClient.user.get_messages(
                chat_id=self._listener.message.chat.id, message_ids=self._listener.mid
            )
            if self._sent_msg is None:
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="Deleted Cmd Message! Don't delete the cmd message again!",
                    disable_notification=True,
                )
        else:
            self._sent_msg = self._listener.message
        return True

    async def _prepare_file(self, file_, dirpath):
        domain_pattern = r'(?i)(?:www\.[a-z0-9-]+\.[a-z]{2,}|[a-z0-9-]+\.(?:cards|fun|biz|com|net|org|site|vip|in|co|tv|xyz|me|cc))'
        cleaned_file = re_sub(domain_pattern, '', file_)
        cleaned_file = re_sub(r'(?i)^[@\s-]+[a-z0-9]+\s*-\s*', '', cleaned_file)
        cleaned_file = re_sub(r'^[-\s@_]+', '', cleaned_file)
        if cleaned_file != file_:
            new_path = ospath.join(dirpath, cleaned_file)
            await rename(self._up_path, new_path)
            self._up_path = new_path
            file_ = cleaned_file

        if self._lprefix:
            self._lprefix = re_sub("<.*?>", "", self._lprefix)
            new_path = ospath.join(dirpath, f"{self._lprefix} {file_}")
            await rename(self._up_path, new_path)
            self._up_path = new_path

        base_filename = file_
        if len(file_) > 60:
            if is_archive(file_):
                name = get_base_name(file_)
                ext = file_.split(name, 1)[1]
            elif match := re_match(r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)", file_):
                name = match.group(0)
                ext = file_.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(file_)) > 1:
                name = fsplit[0]
                ext = fsplit[1]
            else:
                name = file_
                ext = ""
            extn = len(ext)
            remain = 60 - extn
            name = name[:remain]
            new_path = ospath.join(dirpath, f"{name}{ext}")
            await rename(self._up_path, new_path)
            self._up_path = new_path
            base_filename = f"{name}{ext}"

        return base_filename


    def _clean_title(self, filename):
        base = filename.rsplit('.', 1)[0]

        domain_pattern = r'(?i)(?:www\.[a-z0-9-]+\.[a-z]{2,}|[a-z0-9-]+\.(?:cards|fun|biz|com|net|org|site|vip|in|co|tv|xyz|me|cc))'
        base = re_sub(domain_pattern, '', base)

        base = re_sub(r'(?i)^[@\s-]+[a-z0-9]+\s*-\s*', '', base)

        base = re_sub(r'^[-\s@_]+', '', base)
        base = re_sub(r'[-\s_]+$', '', base)

        series_match = re_search(r'(?i)(.*?(?:s\d+[\s\-]*e[p]?\d+|season\s*\d+\s*episode\s*\d+))', base)
        if series_match:
            return f"{series_match.group(1).strip()} - TG: @R_Bots_Updates"

        year_match = re_search(r'(?i)(.*?(?:19\d{2}|20\d{2})\)?)', base)
        if year_match:
            return f"{year_match.group(1).strip()} - TG: @R_Bots_Updates"

        quality_match = re_search(r'(?i)(.*?(?:480p|544p|720p|1080p|1440p|2160p|4k))', base)
        if quality_match:
            return f"{quality_match.group(1).strip()} - TG: @R_Bots_Updates"

        return f"{base.strip()} - TG: @R_Bots_Updates"

    async def _embed_tracks(self):
        if not self._up_path.lower().endswith((".mkv", ".mp4")):
            return

        cmd = ["mediainfo", "--Output=JSON", self._up_path]
        try:
            res = await cmd_exec(cmd)
            data = json.loads(res[0])
            tracks = data.get("media", {}).get("track", [])

            audio_idx = 0
            sub_idx = 0
            metadata_args = []
            has_tracks_to_modify = False

            lang_map = {
                "ta": "Tamil", "hi": "Hindi", "en": "English",
                "ja": "Japanese", "ml": "Malayalam", "te": "Telugu",
                "ko": "Korean", "zh": "Chinese", "ar": "Arabic",
                "kn": "Kannada", "bn": "Bengali", "mr": "Marathi",
                "gu": "Gujarati", "pa": "Punjabi", "ur": "Urdu",
                "or": "Odia", "as": "Assamese", "sa": "Sanskrit",
                "es": "Spanish", "fr": "French", "de": "German",
                "it": "Italian", "pt": "Portuguese", "ru": "Russian",
                "tr": "Turkish", "id": "Indonesian", "th": "Thai",
                "vi": "Vietnamese", "ms": "Malay", "fil": "Filipino"
            }

            codec_map = {
                "Advanced Audio Coding": "AAC",
                "Dolby Digital Plus": "EAC-3",
                "Dolby Digital": "AC-3",
                "Free Lossless Audio Codec": "FLAC",
                "MPEG Audio": "MP3",
                "DTS": "DTS",
            }

            base_filename = ospath.basename(self._up_path)
            parsed_title = self._clean_title(base_filename)
            metadata_args.extend(["-metadata", f"title={parsed_title}"])
            has_tracks_to_modify = True

            for track in tracks:
                track_type = track.get("@type")
                if track_type == "Audio":
                    lang = track.get("Language", "")
                    lang_full = lang_map.get(lang.lower(), lang) if lang else "Unknown"
                    audio_codec = track.get("Format", "")
                    audio_codec_full = codec_map.get(audio_codec, audio_codec) if audio_codec else "Unknown"

                    title = f"{lang_full} ({audio_codec_full}) | @R_Bots_Updates - Search on Telegram"
                    metadata_args.extend(["-metadata:s:a:" + str(audio_idx), f"title={title}"])
                    audio_idx += 1
                    has_tracks_to_modify = True
                elif track_type == "Text":
                    lang = track.get("Language", "")
                    lang_full = lang_map.get(lang.lower(), lang) if lang else "Unknown"

                    title = f"{lang_full} | @R_Bots_Updates - Search on Telegram"
                    metadata_args.extend(["-metadata:s:s:" + str(sub_idx), f"title={title}"])
                    sub_idx += 1
                    has_tracks_to_modify = True

            if not has_tracks_to_modify:
                return

            temp_file = f"{self._up_path}.temp{ospath.splitext(self._up_path)[1]}"

            ffmpeg_cmd = [
                "ffmpeg", "-i", self._up_path,
                "-map", "0", "-c", "copy"
            ] + metadata_args + [temp_file, "-y"]

            _, _, returncode = await cmd_exec(ffmpeg_cmd)

            if returncode == 0 and await aiopath.exists(temp_file):
                # Ensure the original file is replaced successfully
                await rename(temp_file, self._up_path)
            else:
                if await aiopath.exists(temp_file):
                    await remove(temp_file)

        except Exception as e:
            LOGGER.error(f"FFmpeg track embed Error: {e}")
            if await aiopath.exists(f"{self._up_path}.temp{ospath.splitext(self._up_path)[1]}"):
                try:
                    await remove(f"{self._up_path}.temp{ospath.splitext(self._up_path)[1]}")
                except:
                    pass

    def _format_duration(self, ms):
        try:
            seconds = float(ms)
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            s = int(seconds % 60)
            return f"{h:02d}:{m:02d}:{s:02d}"
        except (ValueError, TypeError):
            return "00:00:00"

    async def _generate_caption(self, filename):
        domain_pattern = r'(?i)(?:www\.[a-z0-9-]+\.[a-z]{2,}|[a-z0-9-]+\.(?:cards|fun|biz|com|net|org|site|vip|in|co|tv|xyz|me|cc))'
        filename = re_sub(domain_pattern, '', filename)
        filename = re_sub(r'(?i)^[@\s-]+[a-z0-9]+\s*-\s*', '', filename)
        filename = re_sub(r'^[-\s@_]+', '', filename)
        filename = re_sub(r'[-\s_]+$', '', filename)

        size_str = get_readable_file_size(await aiopath.getsize(self._up_path))
        cmd = ["mediainfo", "--Output=JSON", self._up_path]
        try:
            res = await cmd_exec(cmd)
            data = json.loads(res[0])
            tracks = data.get("media", {}).get("track", [])

            is_video = False
            height = ""
            width = ""
            duration_ms = "0"
            audio_tracks = []
            subtitle_tracks = []

            lang_map = {
                "ta": "Tamil", "hi": "Hindi", "en": "English",
                "ja": "Japanese", "ml": "Malayalam", "te": "Telugu",
                "ko": "Korean", "zh": "Chinese", "ar": "Arabic",
                "kn": "Kannada", "bn": "Bengali", "mr": "Marathi",
                "gu": "Gujarati", "pa": "Punjabi", "ur": "Urdu",
                "or": "Odia", "as": "Assamese", "sa": "Sanskrit",
                "es": "Spanish", "fr": "French", "de": "German",
                "it": "Italian", "pt": "Portuguese", "ru": "Russian",
                "tr": "Turkish", "id": "Indonesian", "th": "Thai",
                "vi": "Vietnamese", "ms": "Malay", "fil": "Filipino"
            }

            codec_map = {
                "Advanced Audio Coding": "AAC",
                "Dolby Digital Plus": "EAC-3",
                "Dolby Digital": "AC-3",
                "Free Lossless Audio Codec": "FLAC",
                "MPEG Audio": "MP3",
                "DTS": "DTS",
            }

            for track in tracks:
                track_type = track.get("@type")
                if track_type == "General" and "Duration" in track:
                    duration_ms = track.get("Duration", "0")
                elif track_type == "Video":
                    is_video = True
                    width = track.get("Width", "")
                    height = track.get("Height", "")
                elif track_type == "Audio":
                    lang = track.get("Language", "")
                    lang_full = lang_map.get(lang.lower(), lang) if lang else ""

                    audio_codec = track.get("Format", "")
                    audio_codec_full = codec_map.get(audio_codec, audio_codec)

                    if lang_full:
                        audio_tracks.append(f"{lang_full} ({audio_codec_full})")
                    else:
                        audio_tracks.append(f"Unknown ({audio_codec_full})")
                elif track_type == "Text":
                    lang = track.get("Language", "")
                    lang_full = lang_map.get(lang.lower(), lang) if lang else ""
                    if lang_full:
                        subtitle_tracks.append(f"{lang_full}")
                    else:
                        subtitle_tracks.append(f"Unknown")

            if not is_video:
                return f"{filename}\n📦 {size_str}"

            duration_str = self._format_duration(duration_ms)

            caption = f"{filename}\n🎬 Quality: {height}p | {width}x{height}\n⏰ Duration: {duration_str}"

            if audio_tracks:
                caption += f"\n🔊 Languages: " + ", ".join(audio_tracks)

            if subtitle_tracks:
                caption += f"\n💬 Subtitles: " + ", ".join(subtitle_tracks)
            else:
                caption += f"\n💬 Subtitles: None"

            return caption
        except Exception as e:
            LOGGER.error(f"MediaInfo Error: {e}")
            return f"{filename}\n📦 {size_str}"

    def _get_input_media(self, subkey, key):
        rlist = []
        for msg in self._media_dict[key][subkey]:
            if key == "videos":
                input_media = InputMediaVideo(
                    media=msg.video.file_id, caption=msg.caption
                )
            else:
                input_media = InputMediaDocument(
                    media=msg.document.file_id, caption=msg.caption
                )
            rlist.append(input_media)
        return rlist

    async def _send_screenshots(self, dirpath, outputs):
        inputs = [
            InputMediaPhoto(ospath.join(dirpath, p), p.rsplit("/", 1)[-1])
            for p in outputs
        ]
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            self._sent_msg = (
                await self._sent_msg.reply_media_group(
                    media=batch,
                    disable_notification=True,
                )
            )[-1]

    async def _send_media_group(self, subkey, key, msgs):
        for index, msg in enumerate(msgs):
            if self._listener.hybrid_leech or not self._user_session:
                msgs[index] = await self._listener.client.get_messages(
                    chat_id=msg[0], message_ids=msg[1]
                )
            else:
                msgs[index] = await TgClient.user.get_messages(
                    chat_id=msg[0], message_ids=msg[1]
                )
        msgs_list = await msgs[0].reply_to_message.reply_media_group(
            media=self._get_input_media(subkey, key),
            disable_notification=True,
        )
        for msg in msgs:
            if msg.link in self._msgs_dict:
                del self._msgs_dict[msg.link]
            await delete_message(msg)
        del self._media_dict[key][subkey]
        if self._files_links and (
            self._listener.is_super_chat or self._listener.up_dest
        ):
            for m in msgs_list:
                self._msgs_dict[m.link] = m.caption
        self._sent_msg = msgs_list[-1]
        if self._base_msg:
            await delete_message(self._base_msg)
            self._base_msg = None

    async def upload(self):
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res:
            return
        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            if dirpath.strip().endswith("_mltbss"):
                await self._send_screenshots(dirpath, files)
                await rmtree(dirpath, ignore_errors=True)
                continue
            for file_ in natsorted(files):
                self._error = ""
                self._up_path = f_path = ospath.join(dirpath, file_)
                if not await aiopath.exists(self._up_path):
                    if intervals["stopAll"]:
                        return
                    LOGGER.error(f"{self._up_path} not exists! Continue uploading!")
                    continue
                try:
                    f_size = await aiopath.getsize(self._up_path)
                    self._total_files += 1
                    if f_size == 0:
                        LOGGER.error(
                            f"{self._up_path} size is zero, telegram don't upload zero size files"
                        )
                        self._corrupted += 1
                        continue
                    if self._listener.is_cancelled:
                        return
                    base_filename = await self._prepare_file(file_, dirpath)
                    await self._embed_tracks()
                    cap_mono = await self._generate_caption(base_filename)
                    if self._last_msg_in_group:
                        group_lists = [
                            x for v in self._media_dict.values() for x in v.keys()
                        ]
                        match = re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", f_path)
                        if not match or match and match.group(0) not in group_lists:
                            for key, value in list(self._media_dict.items()):
                                for subkey, msgs in list(value.items()):
                                    if len(msgs) > 1:
                                        await self._send_media_group(subkey, key, msgs)
                    if self._listener.hybrid_leech and self._listener.user_transmission:
                        self._user_session = f_size > 2097152000
                        if self._user_session:
                            self._sent_msg = await TgClient.user.get_messages(
                                chat_id=self._sent_msg.chat.id,
                                message_ids=self._sent_msg.id,
                            )
                        else:
                            self._sent_msg = await self._listener.client.get_messages(
                                chat_id=self._sent_msg.chat.id,
                                message_ids=self._sent_msg.id,
                            )
                    self._last_msg_in_group = False
                    self._last_uploaded = 0
                    await self._upload_file(cap_mono, file_, f_path)
                    if self._sent_msg and self._sent_msg.media_group_id:
                        for ch, ch_data in list(
                            self._listener.clone_dump_chats.items()
                        ):
                            try:
                                res = await TgClient.bot.copy_message(
                                    chat_id=ch,
                                    from_chat_id=self._sent_msg.chat.id,
                                    message_id=self._sent_msg.id,
                                    message_thread_id=ch_data["thread_id"],
                                    disable_notification=True,
                                    reply_to_message_id=ch_data["last_sent_msg"],
                                )
                                self._listener.clone_dump_chats[ch][
                                    "last_sent_msg"
                                ] = res.id
                            except Exception as e:
                                LOGGER.error(
                                    f"Can't forward message to clone dump chat: {ch}. Error: {e}"
                                )
                    if self._listener.is_cancelled:
                        return
                    if (
                        self._files_links
                        and not self._is_corrupted
                        and (self._listener.is_super_chat or self._listener.up_dest)
                        and not self._is_private
                    ):
                        self._msgs_dict[self._sent_msg.link] = file_
                    await sleep(1)
                except Exception as err:
                    if isinstance(err, RetryError):
                        LOGGER.info(
                            f"Total Attempts: {err.last_attempt.attempt_number}"
                        )
                        err = err.last_attempt.exception()
                    LOGGER.error(f"{err}. Path: {self._up_path}")
                    self._error = str(err)
                    self._corrupted += 1
                    if self._listener.is_cancelled:
                        return
                if not self._listener.is_cancelled and await aiopath.exists(
                    self._up_path
                ):
                    await remove(self._up_path)
        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 1:
                    try:
                        await self._send_media_group(subkey, key, msgs)
                    except Exception as e:
                        LOGGER.info(
                            f"While sending media group at the end of task. Error: {e}"
                        )
        if self._base_msg:
            await delete_message(self._base_msg)
            self._base_msg = None
        if self._listener.is_cancelled:
            return
        if self._total_files == 0:
            await self._listener.on_upload_error(
                "No files to upload. In case you have filled EXCLUDED/INCLUDED EXTENSIONS, then check if all files have those extensions or not."
            )
            return
        if self._total_files <= self._corrupted:
            await self._listener.on_upload_error(
                f"Files Corrupted or unable to upload. {self._error or 'Check logs!'}"
            )
            return
        LOGGER.info(f"Leech Completed: {self._listener.name}")
        await self._listener.on_upload_complete(
            None, self._msgs_dict, self._total_files, self._corrupted
        )
        return

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception),
    )
    async def _upload_file(self, cap_mono, file, o_path, force_document=False):
        if (
            self._thumb is not None
            and not await aiopath.exists(self._thumb)
            and self._thumb != "none"
        ):
            self._thumb = None
        thumb = self._thumb
        self._is_corrupted = False
        try:
            is_video, is_audio, is_image = await get_document_type(self._up_path)

            if not is_image and thumb is None:
                file_name = ospath.splitext(file)[0]
                thumb_path = f"{self._path}/yt-dlp-thumb/{file_name}.jpg"
                if await aiopath.isfile(thumb_path):
                    thumb = thumb_path
                elif await aiopath.isfile(thumb_path.replace("/yt-dlp-thumb", "")):
                    thumb = thumb_path.replace("/yt-dlp-thumb", "")
                elif is_audio and not is_video:
                    thumb = await get_audio_thumbnail(self._up_path)

            if (
                self._listener.as_doc
                or force_document
                or (not is_video and not is_audio and not is_image)
            ):
                key = "documents"
                if is_video and thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, None)

                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_document(
                    document=self._up_path,
                    thumb=thumb,
                    caption=cap_mono,
                    force_document=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_video:
                key = "videos"
                duration = (await get_media_info(self._up_path))[0]
                if thumb is None and self._listener.thumbnail_layout:
                    thumb = await get_multiple_frames_thumbnail(
                        self._up_path,
                        self._listener.thumbnail_layout,
                        self._listener.screen_shots,
                    )
                if thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, duration)
                if thumb is not None and thumb != "none":
                    with Image.open(thumb) as img:
                        width, height = img.size
                else:
                    width = 480
                    height = 320
                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_video(
                    video=self._up_path,
                    caption=cap_mono,
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb,
                    supports_streaming=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_audio:
                key = "audios"
                duration, artist, title = await get_media_info(self._up_path)
                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._sent_msg.reply_audio(
                    audio=self._up_path,
                    caption=cap_mono,
                    duration=duration,
                    performer=artist,
                    title=title,
                    thumb=thumb,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            else:
                key = "photos"
                if self._listener.is_cancelled:
                    return
                self._sent_msg = await self._sent_msg.reply_photo(
                    photo=self._up_path,
                    caption=cap_mono,
                    disable_notification=True,
                    progress=self._upload_progress,
                )

            if (
                not self._listener.is_cancelled
                and self._media_group
                and (self._sent_msg.video or self._sent_msg.document)
            ):
                key = "documents" if self._sent_msg.document else "videos"
                if match := re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", o_path):

                    pname = match.group(0)
                    if pname in self._media_dict[key].keys():
                        self._media_dict[key][pname].append(
                            [self._sent_msg.chat.id, self._sent_msg.id]
                        )
                    else:
                        self._media_dict[key][pname] = [
                            [self._sent_msg.chat.id, self._sent_msg.id]
                        ]
                    msgs = self._media_dict[key][pname]
                    if len(msgs) == 10:
                        await self._send_media_group(pname, key, msgs)
                    else:
                        self._last_msg_in_group = True
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            if self._base_msg and not self._last_msg_in_group:
                await delete_message(self._base_msg)
                self._base_msg = None
        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(str(f))
            await sleep(f.value * 1.3)
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            return await self._upload_file(cap_mono, file, o_path)
        except Exception as err:
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            err_type = "RPCError: " if isinstance(err, RPCError) else ""
            LOGGER.error(f"{err_type}{err}. Path: {self._up_path}")
            if isinstance(err, BadRequest) and key != "documents":
                LOGGER.error(f"Retrying As Document. Path: {self._up_path}")
                return await self._upload_file(cap_mono, file, o_path, True)
            raise err

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except:
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        await self._listener.on_upload_error("your upload has been stopped!")
