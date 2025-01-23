from __future__ import annotations

import array
import asyncio
import contextlib
import io
import logging
import os
import random
import re
import secrets
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum, IntEnum, StrEnum, auto
from logging.handlers import RotatingFileHandler
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import aiofiles
import aiofiles.os
import aiohttp
import cachetools
import interactions
import orjson
from interactions.api.events import ExtensionUnload, MessageCreate, NewThreadCreate
from interactions.client.errors import Forbidden, HTTPException, NotFound

BASE_DIR: str = os.path.dirname(os.path.realpath(__file__))
LOG_FILE: str = os.path.join(BASE_DIR, "lawsuit.log")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s | %(process)d:%(thread)d | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
    "%Y-%m-%d %H:%M:%S.%f %z",
)
file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=1024 * 1024, backupCount=1, encoding="utf-8"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# Schema


class Config:
    GUILD_ID: int = 1150630510696075404
    JUDGE_ROLE_ID: int = 1327208667568799766
    PLAINTIFF_ROLE_ID: int = 1200043628899356702
    COURTROOM_CHANNEL_ID: int = 1247290032881008701
    LOG_CHANNEL_ID: int = 1166627731916734504
    LOG_FORUM_ID: int = 1159097493875871784
    LOG_POST_ID: int = 1279118293936111707
    MAX_JUDGES_PER_LAWSUIT: int = 1
    MAX_JUDGES_PER_APPEAL: int = 3
    MAX_FILE_SIZE: int = 10 * 1024 * 1024
    ALLOWED_MIME_TYPES: Set[str] = {
        "image/jpeg",
        "image/png",
        "application/pdf",
        "text/plain",
    }


class CaseStatus(StrEnum):
    FILED = auto()
    IN_PROGRESS = auto()
    CLOSED = auto()


class EmbedColor(IntEnum):
    OFF = 0x5D5A58
    FATAL = 0xFF4343
    ERROR = 0xE81123
    WARN = 0xFFB900
    INFO = 0x0078D7
    DEBUG = 0x00B7C3
    TRACE = 0x8E8CD8
    ALL = 0x0063B1


class CaseRole(Enum):
    PROSECUTOR = auto()
    PRIVATE_PROSECUTOR = auto()
    PLAINTIFF = auto()
    AGENT = auto()
    DEFENDANT = auto()
    DEFENDER = auto()
    WITNESS = auto()


class EvidenceAction(StrEnum):
    PENDING = auto()
    APPROVED = auto()
    REJECTED = auto()


class RoleAction(Enum):
    ASSIGN = auto()
    REVOKE = auto()


class UserAction(StrEnum):
    MUTE = auto()
    UNMUTE = auto()


class MessageAction(StrEnum):
    PIN = auto()
    DELETE = auto()


class CaseAction(StrEnum):
    FILE = auto()
    CLOSE = auto()
    WITHDRAW = auto()
    REGISTER = auto()
    DISMISS = auto()
    OPEN = auto()


@dataclass
class Attachment:
    url: str
    filename: str
    content_type: str

    def __post_init__(self):
        if (
            not self.filename
            or len(self.filename) > 255
            or not re.match(r'^[^/\\:*?"<>|]+$', self.filename)
        ):
            raise ValueError("Invalid filename")
        if (
            not self.content_type
            or len(self.content_type) > 255
            or not re.match(r"^[\w-]+/[\w-]+$", self.content_type)
        ):
            raise ValueError("Invalid content type")


@dataclass
class Evidence:
    user_id: int
    content: str
    attachments: Tuple[Attachment, ...]
    message_id: int
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0)
    )
    state: EvidenceAction = EvidenceAction.PENDING

    def __post_init__(self):
        if self.user_id <= 0:
            raise ValueError("Invalid user ID")
        if not self.content or len(self.content) > 4000:
            raise ValueError("Invalid content")
        if len(self.attachments) > 10:
            raise ValueError("Too many attachments")
        if self.message_id <= 0:
            raise ValueError("Invalid message ID")


@dataclass
class Data:
    plaintiff_id: int
    defendant_id: int
    accusation: str
    facts: str
    status: CaseStatus = CaseStatus.FILED
    med_thread_id: Optional[int] = None
    judges: Set[int] = field(default_factory=set)
    trial_thread_id: Optional[int] = None
    allowed_roles: Set[int] = field(default_factory=set)
    log_message_id: Optional[int] = None
    mute_list: Set[int] = field(default_factory=set)
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0)
    )
    updated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0)
    )
    evidence_queue: List[Evidence] = field(default_factory=list)
    roles: Dict[str, Set[int]] = field(default_factory=dict)

    def __post_init__(self):
        if self.plaintiff_id <= 0:
            raise ValueError("Invalid plaintiff ID")
        if self.defendant_id <= 0:
            raise ValueError("Invalid defendant ID")
        if self.med_thread_id is not None and self.med_thread_id <= 0:
            raise ValueError("Invalid mediation thread ID")
        if self.trial_thread_id is not None and self.trial_thread_id <= 0:
            raise ValueError("Invalid trial thread ID")
        if self.log_message_id is not None and self.log_message_id <= 0:
            raise ValueError("Invalid log message ID")


class Store:
    def __init__(self) -> None:
        self.db_path = os.path.join(BASE_DIR, "cases.json")
        self.store_initialized = False
        self.store_lock = asyncio.Lock()
        self.store_cache = cachetools.TTLCache(maxsize=1000, ttl=3600)

    async def initialize_store(self) -> None:
        if self.store_initialized:
            return

        async with self.store_lock:
            if not self.store_initialized:
                try:
                    await asyncio.to_thread(
                        os.makedirs, os.path.dirname(self.db_path), 0o755, True
                    )
                    if not os.path.exists(self.db_path):
                        async with aiofiles.open(self.db_path, mode="w") as f:
                            await f.write("{}")
                    self.store_initialized = True
                except OSError as e:
                    logger.critical("Failed to initialize store: %s", repr(e))
                    raise RuntimeError(
                        f"Store initialization failed: {e.__class__.__name__}"
                    ) from e

    async def read_all(self) -> Dict[str, Dict[str, Any]]:
        if data := self.store_cache:
            return dict(data)
        try:
            async with aiofiles.open(self.db_path) as f:
                data = orjson.loads(await f.read())
                self.store_cache.update(data)
                return data
        except Exception as e:
            logger.exception("Failed to read case data: %s", e)
            return {}

    async def read_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        if case_data := self.store_cache.get(case_id):
            return case_data
        try:
            async with aiofiles.open(self.db_path) as f:
                if case_data := orjson.loads(await f.read()).get(case_id):
                    deserialized = self.deserialize_data(case_data)
                    self.store_cache[case_id] = deserialized
                    return deserialized
        except Exception as e:
            logger.exception("Failed to retrieve case %s: %s", case_id, e)
        return None

    def prepare_for_serialization(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            k: (
                sorted(v)
                if isinstance(v, set)
                else (
                    self.prepare_for_serialization(v)
                    if isinstance(v, dict)
                    else (
                        [
                            (
                                self.prepare_for_serialization(i)
                                if isinstance(i, dict)
                                else list(i) if isinstance(i, set) else i
                            )
                            for i in v
                        ]
                        if isinstance(v, (list, tuple))
                        else v
                    )
                )
            )
            for k, v in data.items()
        }

    def deserialize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        set_keys = {"judges", "allowed_roles", "mute_list"}
        return {
            k: (
                set(v)
                if k in set_keys and isinstance(v, list)
                else (
                    self.deserialize_data(v)
                    if isinstance(v, dict)
                    else (
                        [
                            (
                                self.deserialize_data(i)
                                if isinstance(i, dict)
                                else (
                                    set(i)
                                    if isinstance(i, list) and k == "roles"
                                    else i
                                )
                            )
                            for i in v
                        ]
                        if isinstance(v, list)
                        else v
                    )
                )
            )
            for k, v in data.items()
        }

    async def write_case(self, case_id: str, case_data: Dict[str, Any]) -> None:
        serializable = self.prepare_for_serialization(case_data)
        self.store_cache[case_id] = case_data

        try:
            async with aiofiles.open(self.db_path, mode="r+") as f:
                data = orjson.loads(await f.read())
                data[case_id] = serializable
                await f.seek(0)
                await f.truncate()
                await f.write(orjson.dumps(data).decode())
        except Exception as e:
            self.store_cache.pop(case_id, None)
            logger.exception("Failed to persist case %s: %s", case_id, e)
            raise

    async def delete_case(self, case_id: str) -> None:
        try:
            async with aiofiles.open(self.db_path, mode="r+") as f:
                data = orjson.loads(await f.read())
                data.pop(case_id, None)
                await f.seek(0)
                await f.truncate()
                await f.write(orjson.dumps(data).decode())
            self.store_cache.pop(case_id, None)
        except Exception as e:
            logger.exception("Failed to remove case %s: %s", case_id, e)
            raise


class Repo:
    def __init__(self) -> None:
        self.store = Store()
        self.repo_cache = cachetools.TTLCache(maxsize=1024, ttl=3600)
        self.repo_initialized = False
        self.cleanup_task: Optional[asyncio.Task[None]] = None

    async def initialize_repo(self) -> None:
        if self.repo_initialized:
            return
        await self.store.initialize_store()
        self.cleanup_task = asyncio.create_task(self.clean_up())
        self.repo_initialized = True

    async def clean_up(self) -> None:
        while True:
            try:
                await asyncio.sleep(300)
                self.repo_cache.expire()
                self.store.store_cache.expire()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Cache cleanup failed: {e}")
                await asyncio.sleep(60)

    async def ensure_init(self) -> None:
        if not self.repo_initialized:
            await self.initialize_repo()

    async def get_case(self, case_id: str) -> Optional[Data]:
        await self.ensure_init()
        return self.repo_cache.get(case_id) or await self.load_case(case_id)

    async def load_case(self, case_id: str) -> Optional[Data]:
        raw_data = await self.store.read_case(case_id)
        if not raw_data:
            return None

        try:
            data_dict = (
                raw_data
                if isinstance(raw_data, dict)
                else (
                    dict(raw_data)
                    if hasattr(raw_data, "keys") and hasattr(raw_data, "get")
                    else (
                        dict(raw_data)
                        if isinstance(raw_data, (list, tuple))
                        and all(isinstance(x, (list, tuple)) for x in raw_data)
                        else None
                    )
                )
            )
            if not data_dict:
                raise TypeError(f"Incompatible data structure: {type(raw_data)}")

            case = Data(**dict(sorted(data_dict.items())))
            self.repo_cache[case_id] = case
            return case
        except Exception as e:
            logger.exception(f"Failed to validate case data: {e}")
            return None

    async def save_case(self, case_id: str, case: Data) -> None:
        await self.ensure_init()
        await self.store.write_case(case_id, case.__dict__)
        self.repo_cache[case_id] = case

    async def update_case(self, case_id: str, **kwargs: Any) -> Data:
        await self.ensure_init()
        case = await self.get_case(case_id)
        if not case:
            raise ValueError(f"Case {case_id} not found in repository")

        try:
            updated = Data(**{**case.__dict__, **kwargs})
            await self.save_case(case_id, updated)
            return updated
        except Exception as e:
            logger.exception(f"Failed to update case: {e}")
            raise

    async def delete_case(self, case_id: str) -> None:
        await self.ensure_init()
        await self.store.delete_case(case_id)
        self.repo_cache.pop(case_id, None)

    async def get_all_cases(self) -> Dict[str, Data]:
        await self.ensure_init()
        raw_cases = await self.store.read_all()
        return {
            case_id: self.repo_cache.setdefault(
                case_id, Data(**dict(sorted(data.items())))
            )
            for case_id, data in raw_cases.items()
            if self.validate_case_data(data)
        }

    @staticmethod
    def validate_case_data(data: Any) -> bool:
        try:
            Data(**dict(sorted(data.items())))
            return True
        except Exception:
            return False

    def __del__(self) -> None:
        if self.cleanup_task and not self.cleanup_task.done():
            self.cleanup_task.cancel()


# View


class View:

    LEGAL_PROVERBS: Tuple[str, ...] = (
        "Iuris prudentia est divinarum atque humanarum rerum notitia, iusti atque iniusti scientia (D. 1, 1, 10, 2).",
        "Iuri operam daturum prius nosse oportet, unde nomen iuris descendat. est autem a iustitia appellatum: nam, ut eleganter celsus definit, ius est ars boni et aequi (D. 1, 1, 1, pr.).",
        "Iustitia est constans et perpetua voluntas ius suum cuique tribuendi (D. 1, 1, 10, pr.).",
    )

    def __init__(self, bot: interactions.Client) -> None:
        self.bot = bot
        self.config = Config()

    async def create_embed(
        self,
        title: str,
        description: str = "",
        color: EmbedColor = EmbedColor.INFO,
    ) -> interactions.Embed:
        guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        return interactions.Embed(
            title=title,
            description=description,
            color=int(color.value),
            timestamp=interactions.Timestamp.fromdatetime(datetime.now(timezone.utc)),
            footer=interactions.EmbedFooter(
                text=guild.name if guild.icon else "鍵政大舞台",
                icon_url=str(guild.icon.url) if guild.icon else None,
            ),
        )

    async def send_response(
        self,
        ctx: interactions.InteractionContext,
        title: str,
        message: str,
        color: EmbedColor,
    ) -> None:
        await ctx.send(
            embed=await self.create_embed(title, message, color),
            ephemeral=True,
        )

    async def send_error(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.send_response(ctx, "Error", message, EmbedColor.ERROR)

    async def send_success(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.send_response(ctx, "Success", message, EmbedColor.INFO)

    @staticmethod
    def create_lawsuit_form_modal(is_appeal: bool = False) -> interactions.Modal:
        form_type = "appeal" if is_appeal else "lawsuit"
        fields = (
            (
                ("case_number", "Case Number", "Enter the original case number"),
                ("appeal_reason", "Reason for Appeal", "Enter the reason for appeal"),
                ("facts", "Facts", "Describe the relevant facts"),
            )
            if is_appeal
            else (
                ("defendant_id", "Defendant ID", "Enter the defendant's user ID"),
                ("accusation", "Accusation", "Enter the accusation"),
                ("facts", "Facts", "Describe the relevant facts"),
            )
        )

        modal_fields = [
            interactions.ShortText(
                custom_id=f"{field_id}_{i}",
                label=label,
                placeholder=ph,
                min_length=1,
                max_length=4000,
            )
            for i, (field_id, label, ph) in enumerate(fields)
        ]

        return interactions.Modal(
            *modal_fields,
            title=f"{form_type.title()} Form",
            custom_id=f"{form_type}_form_modal",
        )

    @property
    def action_buttons_store(self) -> interactions.ActionRow:
        return interactions.ActionRow(
            interactions.Button(
                style=interactions.ButtonStyle.PRIMARY,
                label="File Lawsuit",
                custom_id="lawsuit_button",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.SECONDARY,
                label="File Appeal",
                custom_id="appeal_button",
            ),
        )

    async def create_summary_embed(
        self, case_id: str, case: Data
    ) -> interactions.Embed:
        fields = {
            "Presiding Judge": " ".join(f"<@{j}>" for j in case.judges)
            or "Not yet assigned",
            "Plaintiff": f"<@{case.plaintiff_id}>",
            "Defendant": f"<@{case.defendant_id}>",
            "Accusation": case.accusation or "None",
            "Facts": case.facts or "None",
        }
        embed = await self.create_embed(f"Case #{case_id}")
        embed.add_fields(
            *[
                interactions.EmbedField(name=k, value=v, inline=True)
                for k, v in fields.items()
            ]
        )
        return embed

    @staticmethod
    def create_button(
        style: interactions.ButtonStyle, label: str, custom_id: str
    ) -> interactions.Button:
        return interactions.Button(style=style, label=label, custom_id=custom_id)

    def create_action_buttons(self, case_id: str) -> List[interactions.ActionRow]:
        return [
            interactions.ActionRow(
                self.create_button(
                    interactions.ButtonStyle.DANGER, "Dismiss", f"dismiss_{case_id}"
                ),
                self.create_button(
                    interactions.ButtonStyle.SUCCESS, "Accept", f"accept_{case_id}"
                ),
                self.create_button(
                    interactions.ButtonStyle.SECONDARY,
                    "Withdraw",
                    f"withdraw_{case_id}",
                ),
            )
        ]

    async def create_trial_visibility_buttons(
        self, case_id: str
    ) -> Tuple[interactions.Button, ...]:
        return (
            self.create_button(
                interactions.ButtonStyle.SUCCESS,
                "Yes",
                f"public_trial_yes_{case_id}",
            ),
            self.create_button(
                interactions.ButtonStyle.DANGER, "No", f"public_trial_no_{case_id}"
            ),
        )

    def create_trial_end_button(self, case_id: str) -> interactions.Button:
        return self.create_button(
            interactions.ButtonStyle.DANGER, "End Trial", f"end_trial_{case_id}"
        )

    def create_user_buttons(self, user_id: int) -> Tuple[interactions.Button, ...]:
        return (
            self.create_button(
                interactions.ButtonStyle.PRIMARY, "Mute", f"mute_{user_id}"
            ),
            self.create_button(
                interactions.ButtonStyle.SUCCESS, "Unmute", f"unmute_{user_id}"
            ),
        )

    def create_message_management_buttons(
        self, message_id: int
    ) -> Tuple[interactions.Button, ...]:
        return (
            self.create_button(
                interactions.ButtonStyle.PRIMARY, "Pin", f"pin_{message_id}"
            ),
            self.create_button(
                interactions.ButtonStyle.DANGER, "Delete", f"delete_{message_id}"
            ),
        )

    def create_evidence_action_buttons(
        self, case_id: str, user_id: int
    ) -> Tuple[interactions.Button, ...]:
        return (
            self.create_button(
                interactions.ButtonStyle.SUCCESS,
                "Make Public",
                f"approve_evidence_{case_id}_{user_id}",
            ),
            self.create_button(
                interactions.ButtonStyle.DANGER,
                "Keep Private",
                f"reject_evidence_{case_id}_{user_id}",
            ),
        )

    def get_daily_proverb(self, current_date: Optional[date] = None) -> str:
        indices = array.array(
            "L",
            random.SystemRandom().sample(
                range(len(self.LEGAL_PROVERBS)), len(self.LEGAL_PROVERBS)
            ),
        )
        return self.LEGAL_PROVERBS[
            indices[
                (current_date or date.today()).toordinal()
                & (len(self.LEGAL_PROVERBS) - 1)
            ]
        ]


# Controller


async def user_is_judge(ctx: interactions.BaseContext) -> bool:
    return any(r.id == Config().JUDGE_ROLE_ID for r in ctx.author.roles)


class Lawsuit(interactions.Extension):
    def __init__(self, bot: interactions.Client) -> None:
        self.bot: interactions.Client = bot
        self.config: Config = Config()
        self.store: Store = Store()
        self.repo: Repo = Repo()
        self.view: View = View(bot)
        self.case_locks: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=1000, ttl=3600
        )
        self.lock_creation_lock: asyncio.Lock = asyncio.Lock()
        self.case_task_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10000)
        self.shutdown_event: asyncio.Event = asyncio.Event()
        self.lawsuit_button_message_id: Optional[int] = None

        self.case_action_handlers: Dict[CaseAction, Callable[..., Awaitable[None]]] = {
            CaseAction.FILE: self.handle_file_case_request,
            CaseAction.REGISTER: self.handle_accept_case_request,
        }

        for closure_action in (
            CaseAction.WITHDRAW,
            CaseAction.CLOSE,
            CaseAction.DISMISS,
        ):
            self.case_action_handlers[closure_action] = (
                lambda ctx, case_id, case, member, action=closure_action: self.handle_case_closure(
                    ctx, case_id, case, member, action
                )
            )

        self.ext_initialized: bool = False
        self.ext_tasks: List[asyncio.Task[Any]] = []

    # Task

    def handle_task_failure(self, task: asyncio.Task[Any]) -> None:
        match (task.cancelled(), task.exception()):
            case (True, _) | (_, asyncio.CancelledError()):
                logger.info(f"Task {task.get_name()} cancelled")
                return
            case (_, exc) if exc:
                logger.critical(f"Task {task.get_name()} failed", exc_info=exc)
                self.restart_background_task(task)

    def restart_background_task(self, task: asyncio.Task[Any]) -> None:
        task_name = task.get_name()
        task_methods = {
            "_fetch_cases": self.run_case_monitor,
            "_daily_embed_update": self.run_embed_updater,
            "_process_task_queue": self.run_task_processor,
        }

        if task_method := task_methods.get(task_name):
            new_task = asyncio.create_task(task_method(), name=task_name)
            new_task.add_done_callback(self.handle_task_failure)
            logger.info(f"Task {task_name} restarted")
        else:
            logger.error(f"Cannot restart task - method not found: {task_name}")

    async def run_case_monitor(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                cases = await asyncio.wait_for(self.repo.get_all_cases(), timeout=30)
                await asyncio.sleep(min(300, max(60, len(cases) >> 1)))
            except (asyncio.TimeoutError, Exception) as e:
                logger.error("Case fetch failed: %s", str(e))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                logger.info("Fetch cases task cancelled")
                return

    async def run_task_processor(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                task = await asyncio.wait_for(self.case_task_queue.get(), timeout=1.0)
                if task:
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(
                                asyncio.create_task(task(), name=f"task_{id(task)}")
                            ),
                            timeout=30.0,
                        )
                    finally:
                        self.case_task_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.exception(f"Task processing failed: {e}")

    async def run_embed_updater(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(86400)
                await self.update_lawsuit_interface()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Embed update failed")
                await asyncio.sleep(60)

    async def update_lawsuit_interface(self) -> None:
        if not self.lawsuit_button_message_id:
            return

        try:
            channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            message = await channel.fetch_message(self.lawsuit_button_message_id)
            embed = await self.view.create_embed(
                title=self.view.get_daily_proverb(),
                description="Click the button to file a lawsuit or appeal.",
            )
            await message.edit(embeds=[embed])
        except Exception:
            logger.exception("Failed to update embed")

    # Listen

    @interactions.listen(NewThreadCreate)
    async def on_thread_creation(self, event: NewThreadCreate) -> None:
        if event.thread.parent_id != self.config.COURTROOM_CHANNEL_ID:
            return

        try:
            async with asyncio.timeout(3.0):
                await self.delete_thread_created_message(
                    event.thread.parent_id, event.thread
                )
        except Exception as e:
            logger.exception(
                f"Thread deletion error: {e}", extra={"thread_id": event.thread.id}
            )

    @interactions.listen(MessageCreate)
    async def on_message_received(self, event: MessageCreate) -> None:
        if event.message.author.id == self.bot.user.id:
            return

        try:
            if isinstance(event.message.channel, interactions.DMChannel):
                await self.handle_evidence_submission(event.message)
            else:
                await self.handle_channel_message(event.message)
        except Exception as e:
            logger.exception(f"Message error: {e}", extra={"msg_id": event.message.id})

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self) -> None:
        self.shutdown_event.set()

        for task in self.ext_tasks:
            if not task.done():
                task.cancel()

        if hasattr(self.bot, "_http"):
            await self.bot._http.close()

        self.ext_initialized = False
        self.ext_tasks.clear()

    # Components

    @interactions.component_callback("lawsuit_button")
    async def initiate_lawsuit(self, ctx: interactions.ComponentContext) -> None:
        if await self.has_ongoing_lawsuit(ctx.author.id):
            return await self.view.send_error(
                ctx, "You already have an ongoing lawsuit."
            )
        await ctx.send_modal(self.view.create_lawsuit_form_modal())

    @interactions.modal_callback("lawsuit_form_modal")
    async def submit_lawsuit_form(self, ctx: interactions.ModalContext) -> None:
        await ctx.defer(ephemeral=True)
        responses = {k: v.strip() for k, v in ctx.responses.items()}

        if not all(responses.values()):
            return await self.view.send_error(ctx, "Please fill all fields.")

        try:
            defendant_id = int(
                "".join(c for c in responses["defendant_id_0"] if c.isdigit())
            )

            if defendant_id == ctx.author.id:
                return await self.view.send_error(
                    ctx, "Self-litigation is not permitted."
                )

            if not await self.bot.fetch_user(defendant_id):
                return await self.view.send_error(ctx, "Invalid defendant ID.")

            case_data = {
                "defendant_id": str(defendant_id),
                "accusation": responses["accusation_1"],
                "facts": responses["facts_2"],
            }

            case_id, case = await self.create_new_case(ctx, case_data, defendant_id)
            if not (
                case_id and case and await self.setup_mediation_thread(case_id, case)
            ):
                return await self.view.send_error(
                    ctx, "Failed to create case/setup mediation."
                )

            if (
                not (updated_case := await self.repo.get_case(case_id))
                or not updated_case.med_thread_id
            ):
                return await self.view.send_error(
                    ctx, "Failed to setup mediation room."
                )

            await self.notify_judges(case_id, ctx.guild_id)
            await self.view.send_success(ctx, f"Mediation room #{case_id} created.")

        except Exception:
            logger.exception("Lawsuit submission failed")
            await self.view.send_error(ctx, "An error occurred.")

    @interactions.component_callback(re.compile(r"dismiss_\d+"))
    async def handle_dismiss(self, ctx: interactions.ComponentContext) -> None:
        await self.process_case_action(ctx, CaseAction.DISMISS, ctx.custom_id[8:])

    @interactions.component_callback(re.compile(r"accept_\d+"))
    async def handle_accept(self, ctx: interactions.ComponentContext) -> None:
        await self.process_case_action(ctx, CaseAction.REGISTER, ctx.custom_id[7:])

    @interactions.component_callback(re.compile(r"withdraw_\d+"))
    async def handle_withdraw(self, ctx: interactions.ComponentContext) -> None:
        await self.process_case_action(ctx, CaseAction.WITHDRAW, ctx.custom_id[9:])

    @interactions.component_callback(re.compile(r"public_trial_(yes|no)_\d+"))
    async def handle_trial_privacy(self, ctx: interactions.ComponentContext) -> None:
        if match := re.match(r"public_trial_(yes|no)_(\d+)", ctx.custom_id):
            is_public, case_id = match.groups()
            if case := await self.repo.get_case(case_id):
                await self.create_trial_thread(ctx, case_id, case, is_public == "yes")
        else:
            await self.view.send_error(ctx, "Invalid trial privacy action.")

    @interactions.component_callback(re.compile(r"(mute|unmute)_\d+"))
    async def handle_user_mute(self, ctx: interactions.ComponentContext) -> None:
        if match := re.match(r"(mute|unmute)_(\d+)", ctx.custom_id):
            action, user_id = match.groups()
            if case_id := await self.validate_user_permissions(ctx.author.id, user_id):
                await self.process_mute_action(
                    ctx, str(case_id), int(user_id), action == "mute"
                )
            else:
                await self.view.send_error(ctx, "Associated case not found.")
        else:
            await self.view.send_error(ctx, "Invalid mute action.")

    @interactions.component_callback(re.compile(r"(pin|delete)_\d+"))
    async def handle_message_action(self, ctx: interactions.ComponentContext) -> None:
        if match := re.match(r"(pin|delete)_(\d+)", ctx.custom_id):
            action, message_id = match.groups()
            if case_id := await self.validate_user_permissions(
                ctx.author.id, message_id
            ):
                await self.process_message_action(
                    ctx, str(case_id), int(message_id), MessageAction(action)
                )
            else:
                await self.view.send_error(ctx, "Associated case not found.")
        else:
            await self.view.send_error(ctx, "Invalid message action.")

    @interactions.component_callback(re.compile(r"(approve|reject)_evidence_\d+_\d+"))
    async def handle_evidence_action(self, ctx: interactions.ComponentContext) -> None:
        if match := re.match(r"(approve|reject)_evidence_(\d+)_(\d+)", ctx.custom_id):
            action, case_id, user_id = match.groups()
            await self.process_evidence_decision(
                ctx,
                (
                    EvidenceAction.APPROVED
                    if action == "approve"
                    else EvidenceAction.REJECTED
                ),
                case_id,
                int(user_id),
            )
        else:
            await self.view.send_error(ctx, "Invalid evidence action.")

    @interactions.component_callback(re.compile(r"end_trial_\d+"))
    async def handle_end_trial(self, ctx: interactions.ComponentContext) -> None:
        await self.process_case_action(ctx, CaseAction.CLOSE, ctx.custom_id[10:])

    # Commands

    module_base = interactions.SlashCommand(
        name="lawsuit",
        description="Lawsuit management system",
    )

    @module_base.subcommand("init", sub_cmd_description="Initialize the lawsuit system")
    @interactions.check(user_is_judge)
    async def init_lawsuit(self, ctx: interactions.SlashContext) -> None:
        if self.ext_initialized:
            return await self.view.send_success(ctx, "System already initialized.")

        try:
            await ctx.defer(ephemeral=True)
            await self.repo.initialize_repo()

            self.ext_tasks = [
                asyncio.create_task(coro(), name=name)
                for name, coro in {
                    "_fetch_cases": self.run_case_monitor,
                    "_daily_embed_update": self.run_embed_updater,
                    "_process_task_queue": self.run_task_processor,
                }.items()
            ]
            for task in self.ext_tasks:
                task.add_done_callback(self.handle_task_failure)

            self.ext_initialized = True
            await self.view.send_success(ctx, "Lawsuit system initialized.")

        except Exception:
            await self.view.send_error(ctx, "Failed to initialize system.")
            raise

    @module_base.subcommand(
        "dispatch", sub_cmd_description="Deploy the lawsuit management interface"
    )
    @interactions.check(user_is_judge)
    async def dispatch(self, ctx: interactions.SlashContext) -> None:
        await ctx.defer(ephemeral=True)
        try:
            embed = await self.view.create_embed(
                title=self.view.get_daily_proverb(),
                description="Click below to file a lawsuit or appeal.",
            )

            msg = await ctx.channel.send(
                embeds=[embed],
                components=[self.view.action_buttons_store],
                allowed_mentions=interactions.AllowedMentions.none(),
            )

            self.lawsuit_button_message_id = msg.id
            await self.initialize_chat_thread(ctx)

            logger.info(
                "Lawsuit interface deployed",
                extra={
                    "user_id": ctx.author.id,
                    "channel_id": ctx.channel_id,
                    "message_id": msg.id,
                },
            )

        except Exception:
            logger.exception("Failed to deploy interface")
            await self.view.send_error(ctx, "Failed to deploy interface.")
            raise

    @module_base.subcommand(
        "role", sub_cmd_description="Manage user roles within a case"
    )
    @interactions.slash_option(
        name="action",
        description="Role management action",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            interactions.SlashCommandChoice(name=a.name.title(), value=str(a.value))
            for a in RoleAction
        ],
    )
    @interactions.slash_option(
        name="role",
        description="Target role",
        opt_type=interactions.OptionType.STRING,
        required=True,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="user",
        description="Target user",
        opt_type=interactions.OptionType.USER,
        required=True,
    )
    async def manage_role(
        self,
        ctx: interactions.SlashContext,
        action: str,
        role: str,
        user: interactions.User,
    ) -> None:
        try:
            if not (
                case_id := await self.find_case_number_by_channel_id(ctx.channel_id)
            ):
                return await self.view.send_error(ctx, "No associated case found.")

            action_enum = RoleAction(action.lower())
            await self.manage_case_role(ctx, str(case_id), action_enum, role, user)

            logger.info(
                "Role management completed.",
                extra={
                    "action": (
                        "assigned to"
                        if action_enum == RoleAction.ASSIGN
                        else "revoked from"
                    ),
                    "role": role,
                    "user_id": user.id,
                    "case_id": case_id,
                    "moderator_id": ctx.author.id,
                },
            )

        except Exception as e:
            logger.exception(f"Operation failed: {str(e)}")
            await self.view.send_error(ctx, "Role management failed.")

    @interactions.message_context_menu(name="Message in Lawsuit")
    async def message_menu(self, ctx: interactions.ContextMenuContext) -> None:
        try:
            if not await user_is_judge(ctx):
                return await self.view.send_error(
                    ctx, "Only judges can manage messages."
                )

            if not (
                case_id := await self.find_case_number_by_channel_id(ctx.channel_id)
            ):
                return await self.view.send_error(
                    ctx, "This command can only be used in case threads."
                )

            await ctx.send(
                "Select an action:",
                components=[
                    interactions.ActionRow(
                        *self.view.create_message_management_buttons(ctx.target.id)
                    )
                ],
                ephemeral=True,
            )

            logger.info(
                "Message management menu opened",
                extra={
                    "case_id": case_id,
                    "message_id": ctx.target.id,
                    "user_id": ctx.author.id,
                },
            )

        except Exception:
            logger.exception("Failed to open message management menu")
            await self.view.send_error(
                ctx, "Failed to load message management options."
            )
            raise

    @interactions.user_context_menu(name="User in Lawsuit")
    async def user_menu(self, ctx: interactions.ContextMenuContext) -> None:
        try:
            if not await user_is_judge(ctx):
                return await self.view.send_error(ctx, "Only judges can manage users.")

            if not (
                case_id := await self.find_case_number_by_channel_id(ctx.channel_id)
            ):
                return await self.view.send_error(
                    ctx, "This command can only be used in case threads."
                )

            await ctx.send(
                "Select an action:",
                components=[
                    interactions.ActionRow(
                        *self.view.create_user_buttons(ctx.target.id)
                    )
                ],
                ephemeral=True,
            )

            logger.info(
                "User management menu opened",
                extra={
                    "case_id": case_id,
                    "target_user_id": ctx.target.id,
                    "user_id": ctx.author.id,
                },
            )

        except Exception:
            logger.exception("Failed to open user management menu")
            await self.view.send_error(ctx, "Failed to load user management options.")
            raise

    # Service

    async def process_case_action(
        self,
        ctx: interactions.ComponentContext,
        action: CaseAction,
        case_id: str,
    ) -> None:
        try:
            case = await self.repo.get_case(case_id)
            guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            if not case:
                return await self.view.send_error(ctx, "Case not found.")

            member = await guild.fetch_member(ctx.author.id)
            if handler := self.case_action_handlers.get(action):
                async with asyncio.timeout(5.0):
                    await handler(ctx, case_id, case, member)
            else:
                await self.view.send_error(ctx, "Invalid action specified.")

        except asyncio.TimeoutError:
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
        except Exception as e:
            await self.view.send_error(
                ctx, f"An unexpected error occurred: {type(e).__name__}"
            )

    async def handle_file_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        user: Union[interactions.Member, interactions.User],
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                if await user_is_judge(interactions.BaseContext(user)):
                    await self.add_judge_to_case(ctx, case_id)
                else:
                    await self.view.send_error(ctx, "Insufficient permissions.")
        except Exception as e:
            await self.view.send_error(ctx, f"An error occurred: {type(e).__name__}")
            raise

    async def handle_case_closure(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        member: interactions.Member,
        action: CaseAction,
    ) -> None:
        try:
            if not await self.user_has_permission_for_closure(
                ctx.author.id, case_id, member
            ):
                return await self.view.send_error(ctx, "Insufficient permissions.")

            action_name = action.value.capitalize()
            case_status = CaseStatus(action.value)

            setattr(case, "status", case_status)
            thread_ids = {
                tid
                for tid in (
                    getattr(case, "med_thread_id", None),
                    getattr(case, "trial_thread_id", None),
                )
                if tid
            }

            for tid in thread_ids:
                await self.archive_thread(tid)

            notification = f"Case number {case_id} has been {action_name} by {ctx.author.display_name}."
            await self.notify_participants(case_id, notification)
            await self.enqueue_case_file_save(case_id, case)
            await self.view.send_success(
                ctx, f"Case number {case_id} successfully {action_name}."
            )

        except Exception as e:
            await self.view.send_error(
                ctx, f"Failed to close the case: {type(e).__name__}"
            )
            raise

    async def handle_accept_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                if not await self.is_user_assigned_to_case(
                    ctx.author.id, case_id, "judges"
                ):
                    return await self.view.send_error(ctx, "Insufficient permissions.")

                embed = await self.view.create_embed(title="Public or Private Trial")
                buttons = await self.view.create_trial_visibility_buttons(case_id)
                await ctx.send(
                    embeds=[embed],
                    components=[interactions.ActionRow(*buttons)],
                    allowed_mentions=interactions.AllowedMentions.none(),
                )
        except Exception:
            await self.view.send_error(ctx, "Failed to process request.")

    async def end_trial(self, ctx: interactions.ComponentContext) -> None:
        try:
            async with asyncio.timeout(5.0):
                if match := re.match(
                    r"^end_trial_([a-f0-9]+)$", ctx.custom_id, re.IGNORECASE
                ):
                    await self.process_case_action(ctx, CaseAction.CLOSE, match[1])
                else:
                    await self.view.send_error(ctx, "Invalid interaction data.")
        except Exception:
            await self.view.send_error(ctx, "Failed to end trial.")

    # Thread

    @staticmethod
    async def add_members_to_thread(
        thread: interactions.ThreadChannel, member_ids: Set[int]
    ) -> None:
        async def _add_member(member_id: int) -> None:
            try:
                async with asyncio.timeout(5.0):
                    member = await thread.guild.fetch_member(member_id)
                    await thread.add_member(member)
                    logger.debug(
                        "Added member to thread", extra={"member_id": member_id}
                    )
            except Exception:
                logger.exception(
                    "Failed to add member to thread", extra={"member_id": member_id}
                )

        await asyncio.gather(*[_add_member(mid) for mid in member_ids])

    async def add_judge_to_case(
        self, ctx: interactions.ComponentContext, case_id: str
    ) -> None:
        try:
            case = await self.repo.get_case(case_id)
            if not case or not hasattr(case, "judges"):
                return await self.view.send_error(ctx, "Invalid case data structure.")

            judges: set[int] = getattr(case, "judges", set())
            if len(judges) >= self.config.MAX_JUDGES_PER_LAWSUIT:
                return await self.view.send_error(
                    ctx, "Maximum number of judges has been reached."
                )
            if ctx.author.id in judges:
                return await self.view.send_error(
                    ctx, "You are already assigned as a judge to this case."
                )

            judges.add(ctx.author.id)
            notification = f"{ctx.author.display_name} has been appointed as judge for Case #{case_id}."

            await asyncio.gather(
                self.repo.update_case(case_id, judges=judges),
                self.notify_participants(case_id, notification),
                self.update_thread_permissions(case_id, ctx.author.id),
                self.view.send_success(ctx, f"Assigned as judge to Case #{case_id}."),
            )

        except Exception:
            logger.exception("Judge assignment failed", extra={"case_id": case_id})
            await self.view.send_error(ctx, "Failed to assign judge role.")
            raise

    async def update_thread_permissions(self, case_id: str, judge_id: int) -> None:
        try:
            case = await self.repo.get_case(case_id)
            thread_ids = {
                tid
                for tid in ("med_thread_id", "trial_thread_id")
                if hasattr(case, tid) and getattr(case, tid)
            }
            if not thread_ids:
                return

            perms = interactions.PermissionOverwrite(
                id=interactions.Snowflake(judge_id),
                allow=interactions.Permissions.VIEW_CHANNEL
                | interactions.Permissions.SEND_MESSAGES,
                type=interactions.OverwriteType.MEMBER,
            )

            await asyncio.gather(
                *[
                    (await self.bot.fetch_channel(tid)).edit_permission(perms)
                    for tid in thread_ids
                ]
            )

        except Exception:
            logger.exception("Permission update failed", extra={"case_id": case_id})
            raise

    # User

    async def process_mute_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        mute: bool,
    ) -> None:
        async with self.acquire_case_lock(case_id):
            if not (case := await self.repo.get_case(case_id)):
                raise ValueError(f"Case {case_id} not found")

            mute_list: set[int] = getattr(case, "mute_list", set())
            action_str = ("un" * (not mute)) + "muted"

            if (target_id in mute_list) == mute:
                await self.view.send_error(
                    ctx, f"User <@{target_id}> is already {action_str}."
                )
                return

            notification = (
                f"User <@{target_id}> has been {action_str} by <@{ctx.author.id}>."
            )

            mute_list = mute_list | {target_id} if mute else mute_list - {target_id}

            await self.repo.update_case(case_id, mute_list=tuple(sorted(mute_list)))
            await self.view.send_success(ctx, notification)
            await self.notify_participants(case_id, notification)

    async def process_message_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        action_type: MessageAction,
    ) -> None:
        try:
            if not (
                message := await (
                    await self.bot.fetch_channel(ctx.channel_id)
                ).fetch_message(target_id)
            ):
                await self.view.send_error(
                    ctx, f"Message not found in Case #{case_id}."
                )
                return

            action_handlers = {
                MessageAction.PIN: message.pin,
                MessageAction.DELETE: message.delete,
            }

            await action_handlers[action_type]()
            action_str = "pinned" if action_type == MessageAction.PIN else "deleted"
            notification = f"A message has been {action_str} by <@{ctx.author.id}> in Case #{case_id}."

            await self.view.send_success(
                ctx, f"Message has been {action_str} successfully."
            )
            await self.notify_participants(case_id, notification)

        except (NotFound, Forbidden, HTTPException) as e:
            error_messages = {
                NotFound: f"Message not found in Case #{case_id}.",
                Forbidden: f"Insufficient permissions to {action_type.value} the message.",
                HTTPException: f"Failed to {action_type.value} message: {str(e)}",
            }
            await self.view.send_error(ctx, error_messages[type(e)])

    # Notification

    async def notify_evidence(self, user_id: int, case_id: str, decision: str) -> None:
        embed = await self.view.create_embed(
            title="Evidence Decision",
            description=f"Your evidence submission for Case {case_id} has been {decision}.",
        )
        await self.send_dm(user_id, embed=embed)

    async def notify_participants(self, case_id: str, message: str) -> None:
        if case := await self.repo.get_case(case_id):
            participants = {case.plaintiff_id, case.defendant_id} | case.judges - {None}
            embed = await self.view.create_embed(
                title=f"Case {case_id} Update", description=message
            )
            await self.notify_users(
                participants, lambda uid: self.send_dm(uid, embed=embed)
            )
        else:
            logger.warning(
                "Failed to locate case %s during participant notification", case_id
            )

    async def notify_judges_for_evidence(self, case_id: str, user_id: int) -> None:
        if case := await self.repo.get_case(case_id):
            embed = await self.view.create_embed(
                title="Evidence Submission",
                description=f"New evidence has been submitted for Case {case_id}. Your review is required.",
            )
            buttons = self.view.create_evidence_action_buttons(case_id, user_id)
            await self.notify_users(
                case.judges,
                lambda uid: self.send_dm(
                    uid, embed=embed, components=[interactions.ActionRow(*buttons)]
                ),
            )
        else:
            logger.warning(
                "Unable to locate case %s for evidence notification", case_id
            )

    async def notify_judges(self, case_id: str, guild_id: int) -> None:
        guild, case = await asyncio.gather(
            self.bot.fetch_guild(guild_id), self.repo.get_case(case_id)
        )

        if not (case and case.med_thread_id):
            logger.warning("Mediation thread not found for case %s", case_id)
            return

        judge_ids = {
            m.id
            for m in guild.members
            if self.config.JUDGE_ROLE_ID in {r.id for r in m.roles}
        }
        if not judge_ids:
            logger.warning("No qualified judges found for case %s", case_id)
            return

        thread_link = f"https://discord.com/channels/{guild_id}/{case.med_thread_id}"
        embed = await self.view.create_embed(
            title=f"Case #{case_id} Complaint Received",
            description=f"Your presence is requested in the [mediation room]({thread_link}).",
        )
        components = [
            interactions.ActionRow(
                interactions.Button(
                    custom_id=f"register_{case_id}",
                    style=interactions.ButtonStyle.PRIMARY,
                    label="Join Mediation",
                )
            )
        ]

        await self.notify_users(
            judge_ids, lambda uid: self.send_dm(uid, embed=embed, components=components)
        )

    @staticmethod
    async def notify_users(
        user_ids: set[int], notify_func: Callable[[int], Coroutine]
    ) -> None:
        if not user_ids:
            return
        await asyncio.gather(*(notify_func(uid) for uid in user_ids))

    async def send_dm(self, user_id: int, **kwargs: Any) -> None:
        try:
            async with asyncio.timeout(5.0):
                await (await self.bot.fetch_user(user_id)).send(**kwargs)
                logger.debug("Direct message delivered", extra={"user_id": user_id})
        except Exception as e:
            logger.log(
                (
                    logging.WARNING
                    if isinstance(e, (asyncio.TimeoutError, HTTPException))
                    else logging.ERROR
                ),
                "Direct message delivery failed",
                extra={"user_id": user_id},
                exc_info=True,
            )

    # Role

    @manage_role.autocomplete("role")
    async def role_autocomplete(self, ctx: interactions.AutocompleteContext) -> None:
        input_text = ctx.input_text.lower()
        await ctx.send(
            [
                {"name": role.name, "value": role.name}
                for role in CaseRole
                if input_text in role.name.lower()
            ][:25]
        )

    async def manage_case_role(
        self,
        ctx: interactions.SlashContext,
        case_id: str,
        action: RoleAction,
        role: str,
        user: interactions.User,
    ) -> None:
        try:
            async with asyncio.timeout(5.0), self.acquire_case_lock(case_id):
                case = await self.repo.get_case(case_id)
                if not case:
                    raise ValueError(f"Case `{case_id}` does not exist.")

                if not await self.is_user_assigned_to_case(
                    ctx.author.id, case_id, "judges"
                ):
                    raise PermissionError(
                        "Insufficient privileges to manage case roles."
                    )

                role_enum = CaseRole[role.upper()]
                users_with_role = case.roles.setdefault(role_enum.name, set())

                if (
                    user_in_role := user.id in users_with_role
                ) and action == RoleAction.ASSIGN:
                    raise ValueError(
                        f"User <@{user.id}> is already assigned the role `{role_enum.name}`."
                    )
                elif not user_in_role and action == RoleAction.REVOKE:
                    raise ValueError(
                        f"User <@{user.id}> does not currently hold the role `{role_enum.name}`."
                    )

                if action == RoleAction.ASSIGN:
                    users_with_role.add(user.id)
                else:
                    users_with_role.remove(user.id)

                operation = (
                    "assigned to" if action == RoleAction.ASSIGN else "removed from"
                )

                await self.repo.update_case(case_id, roles=case.roles)

                notification = f"<@{ctx.author.id}> has {operation} <@{user.id}> the role of `{role_enum.name}`."
                success_msg = f"Successfully {operation} <@{user.id}> the role `{role_enum.name}`."

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.view.send_success(ctx, success_msg))
                    tg.create_task(self.notify_participants(case_id, notification))

        except (ValueError, PermissionError) as e:
            logger.warning("Role management failed for case '%s': %s", case_id, e)
            await self.view.send_error(ctx, str(e))
        except asyncio.TimeoutError:
            logger.error("Lock acquisition timeout for case '%s'", case_id)
            await self.view.send_error(
                ctx, "Operation timed out. Please try again later."
            )
        except Exception as e:
            logger.exception(
                "Unexpected error during role management for case '%s': %s", case_id, e
            )
            await self.view.send_error(
                ctx, "An internal error occurred. Please contact support."
            )

    # Evidence

    async def process_evidence_decision(
        self,
        ctx: interactions.ComponentContext,
        action: EvidenceAction,
        case_id: str,
        user_id: int,
    ) -> None:
        case = await self.repo.get_case(case_id)
        if not case:
            await self.view.send_error(ctx, "Case not found.")
            return

        evidence = next((e for e in case.evidence_queue if e.user_id == user_id), None)
        if not evidence:
            await self.view.send_error(ctx, "Evidence not found in queue.")
            return

        evidence.state = action
        evidence_list = f"evidence_{action.value.lower()}"

        setattr(case, evidence_list, [*getattr(case, evidence_list, []), evidence])
        case.evidence_queue = [e for e in case.evidence_queue if e.user_id != user_id]

        await self.repo.update_case(
            case_id,
            evidence_queue=case.evidence_queue,
            **{evidence_list: getattr(case, evidence_list)},
        )

        await self.notify_participants(
            case_id,
            f"<@{user_id}>'s evidence has been {action.value.lower()} by <@{ctx.author.id}>.",
        )
        await self.notify_evidence(user_id, case_id, action.value.lower())
        await self.handle_evidence_finalization(ctx, case, evidence, action)
        await self.view.send_success(
            ctx, f"Evidence {action.value.lower()} successfully."
        )

    async def handle_evidence_finalization(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
        action: EvidenceAction,
    ) -> None:
        await (
            self.publish_evidence(ctx, case, evidence)
            if action is EvidenceAction.APPROVED
            else self.notify_rejection(ctx, evidence.user_id)
        )

    async def publish_evidence(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
    ) -> None:
        if not case.trial_thread_id:
            await self.view.send_success(ctx, "Evidence approved and queued for trial.")
            return

        try:
            thread = await self.bot.fetch_channel(case.trial_thread_id)
            files = [
                f
                for f in await asyncio.gather(
                    *(
                        self.fetch_attachment_as_file(att)
                        for att in evidence.attachments
                    ),
                    return_exceptions=True,
                )
                if not isinstance(f, Exception) and f
            ]

            await thread.send(
                content=f"Evidence from <@{evidence.user_id}>:\n{evidence.content}",
                files=files,
            )
            await self.view.send_success(ctx, "Evidence published to trial thread.")
        except Exception as e:
            logger.exception(f"Failed to publish evidence: {e}")
            await self.view.send_error(
                ctx, "Failed to publish evidence to trial thread."
            )

    @staticmethod
    async def fetch_attachment_as_file(
        attachment: Attachment,
    ) -> Optional[interactions.File]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(attachment.url) as response:
                    if not response.ok or not (content := await response.read()):
                        return None
                    return interactions.File(
                        file=io.BytesIO(content),
                        file_name=attachment.filename,
                        description=f"Evidence attachment: {attachment.filename}",
                    )
        except Exception as e:
            logger.exception(
                f"Error fetching attachment: {e}", extra={"url": attachment.url}
            )
            return None

    async def notify_rejection(
        self,
        ctx: interactions.ComponentContext,
        user_id: int,
    ) -> None:
        try:
            user = await self.bot.fetch_user(user_id)
            await user.send("Your submitted evidence has been rejected.")
            await self.view.send_success(ctx, "Evidence rejection notification sent.")
        except Exception as e:
            logger.exception(f"Failed to notify user of rejection: {e}")
            await self.view.send_error(ctx, "Failed to send rejection notification.")

    async def handle_evidence_submission(
        self,
        message: interactions.Message,
    ) -> None:
        try:
            if not (case_id := await self.get_active_case_for_user(message.author.id)):
                await message.author.send(
                    "No active cases found for evidence submission."
                )
                return

            evidence = Evidence(
                user_id=message.author.id,
                content=self.sanitize_text(message.content),
                attachments=tuple(
                    Attachment(
                        url=a.url, filename=a.filename, content_type=a.content_type
                    )
                    for a in message.attachments
                    if self.validate_attachment(a, self.config.MAX_FILE_SIZE)
                ),
                message_id=message.id,
                timestamp=message.created_at,
            )

            await self.process_case_evidence(case_id, evidence)
            await message.author.send("Evidence submitted for review.")
        except Exception as e:
            logger.exception(f"Failed to process evidence submission: {e}")
            await message.author.send("Failed to process evidence submission.")

    async def handle_channel_message(
        self,
        message: interactions.Message,
    ) -> None:
        if not (
            case_id := await self.find_case_number_by_channel_id(message.channel.id)
        ):
            return

        try:
            if (
                case := await self.repo.get_case(case_id)
            ) and message.author.id in case.mute_list:
                await message.delete()
        except Exception as e:
            logger.exception(f"Failed to process channel message: {e}")

    async def process_case_evidence(
        self,
        case_id: str,
        evidence: Evidence,
    ) -> None:
        case = await self.repo.get_case(case_id)
        if not case or case.status not in {CaseStatus.FILED, CaseStatus.IN_PROGRESS}:
            raise ValueError("Case is not currently accepting evidence.")

        case.evidence_queue.append(evidence)
        await self.repo.update_case(case_id, evidence_queue=case.evidence_queue)
        await self.notify_judges_for_evidence(case_id, evidence.user_id)

    # Validation

    async def user_has_judge_role(
        self, user: Union[interactions.Member, interactions.User]
    ) -> bool:
        return self.config.JUDGE_ROLE_ID in {
            r.id
            for r in (
                user
                if isinstance(user, interactions.Member)
                else await (
                    await self.bot.fetch_guild(self.config.GUILD_ID)
                ).fetch_member(user.id)
            ).roles
        }

    async def user_has_permission_for_closure(
        self, user_id: int, case_id: str, member: interactions.Member
    ) -> bool:
        return self.config.JUDGE_ROLE_ID in {
            r.id for r in member.roles
        } and await self.is_user_assigned_to_case(user_id, case_id, "plaintiff_id")

    def validate_attachment(
        self, attachment: interactions.Attachment, max_size: int
    ) -> bool:
        return (
            not attachment.content_type.startswith("image/")
            or attachment.content_type in self.config.ALLOWED_MIME_TYPES
            and attachment.size <= max_size
            and bool(attachment.filename)
            and attachment.height is not None
        )

    @staticmethod
    def get_role_check(role: str) -> Callable[[Data, int], bool]:
        return {
            "plaintiff_id": lambda c, u: c.plaintiff_id == u,
            "defendant_id": lambda c, u: c.defendant_id == u,
            "judges": lambda c, u: u in c.judges,
            "witnesses": lambda c, u: u in c.witnesses,
            "attorneys": lambda c, u: u in c.attorneys,
        }.get(role, lambda c, u: u in getattr(c, role, set()))

    async def is_user_assigned_to_case(
        self, user_id: int, case_id: str, role: str
    ) -> bool:
        try:
            return bool(
                case and self.get_role_check(role)(case, user_id)
                if (case := await self.repo.get_case(case_id))
                else False
            )
        except Exception:
            logger.exception(
                "Assignment verification failed",
                extra={"user": user_id, "case": case_id},
            )
            return False

    async def has_ongoing_lawsuit(self, user_id: int) -> bool:
        return any(
            case.plaintiff_id == user_id and case.status != CaseStatus.CLOSED
            for case in (await self.repo.get_all_cases()).values()
        )

    async def validate_user_permissions(self, user_id: int, case_id: str) -> bool:
        try:
            return bool(
                case
                and (
                    user_id in case.judges
                    or user_id in {case.plaintiff_id, case.defendant_id}
                )
                if (case := await self.repo.get_case(case_id))
                else False
            )
        except Exception:
            logger.exception(
                "Permission validation failed", extra={"user": user_id, "case": case_id}
            )
            return False

    # Helper

    async def archive_thread(self, med_thread_id: int) -> bool:
        try:
            if thread := await self.bot.fetch_channel(med_thread_id):
                return isinstance(
                    thread, interactions.ThreadChannel
                ) and await thread.edit(archived=True, locked=True)
            return False
        except Exception:
            logger.exception(
                "Failed to archive thread", extra={"med_thread_id": med_thread_id}
            )
            return False

    @staticmethod
    def sanitize_text(text: str, max_length: int = 2000) -> str:
        return (text and re.sub(r"\s+", " ", text).strip()[:max_length]) or ""

    async def delete_thread_created_message(
        self, channel_id: int, thread: interactions.ThreadChannel
    ) -> bool:
        try:
            if not isinstance(
                channel := await self.bot.fetch_channel(channel_id),
                interactions.GuildText,
            ):
                return False
            async for message in channel.history(limit=5):
                if (
                    message.type == interactions.MessageType.THREAD_CREATED
                    and message.thread.id == thread.id
                ):
                    try:
                        return await message.delete() or True
                    except Exception:
                        logger.warning(
                            "Failed to delete message", extra={"message_id": message.id}
                        )
                        return False
            return False
        except Exception:
            logger.exception(
                "Failed to delete thread message", extra={"med_thread_id": thread.id}
            )
            return False

    async def initialize_chat_thread(self, ctx: interactions.SlashContext) -> bool:
        try:
            if not isinstance(
                channel := await self.bot.fetch_channel(
                    self.config.COURTROOM_CHANNEL_ID
                ),
                interactions.GuildText,
            ):
                return False
            thread = await self.get_or_create_chat_thread(channel)
            embed = await self.create_chat_room_setup_embed(thread)
            await self.view.send_success(ctx, embed.description)
            return True
        except Exception:
            logger.exception("Chat thread initialization failed")
            return False

    async def setup_mediation_thread(self, case_id: str, case: Data) -> bool:
        try:
            channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            if not (guild := await self.bot.fetch_guild(self.config.GUILD_ID)):
                return False

            thread = await self.create_mediation_thread(channel, case_id)
            async with self.acquire_case_lock(case_id) as locked_case:
                if not locked_case:
                    return False
                await self.repo.update_case(case_id, med_thread_id=thread.id)
                judges = await self.get_judges(guild)
                participants = {case.plaintiff_id, case.defendant_id} | judges
                await asyncio.gather(
                    self.send_case_summary(thread, case_id, case),
                    self.add_members_to_thread(thread, participants),
                )
            return True
        except Exception as e:
            logger.exception(f"Mediation setup failed: {e}", extra={"case_id": case_id})
            return False

    async def send_case_summary(
        self, thread: interactions.ThreadChannel, case_id: str, case: Data
    ) -> bool:
        try:
            return bool(
                await thread.send(
                    embeds=[await self.view.create_summary_embed(case_id, case)],
                    components=self.view.create_action_buttons(case_id),
                    allowed_mentions=interactions.AllowedMentions.none(),
                )
            )
        except Exception:
            logger.exception("Failed to send case summary", extra={"case_id": case_id})
            return False

    # Initialization

    @staticmethod
    async def create_mediation_thread(
        channel: interactions.GuildText, case_id: str
    ) -> interactions.ThreadChannel:
        try:
            return await channel.create_thread(
                name=f"Case #{case_id} Mediation Room",
                thread_type=interactions.ChannelType.GUILD_PRIVATE_THREAD,
                reason=f"Mediation thread for case {case_id}",
            )
        except HTTPException:
            logger.error(
                "Failed to create mediation thread", extra={"case_id": case_id}
            )
            raise
        except Exception as e:
            logger.exception(f"Unexpected error while creating mediation thread: {e}")
            raise

    @staticmethod
    def create_case_id(length: int = 6) -> str:
        return secrets.token_urlsafe(length // 2)[:length]

    async def create_new_case(
        self, ctx: interactions.ModalContext, data: Dict[str, str], defendant_id: int
    ) -> Tuple[Optional[str], Optional[Data]]:
        try:
            case_id = self.create_case_id()
            case = await self.create_case_data(ctx, data, defendant_id)
            await self.repo.save_case(case_id, case)
            return case_id, case
        except Exception:
            logger.exception("Case creation failed")
            return None, None

    async def create_case_data(
        self, ctx: interactions.ModalContext, data: Dict[str, str], defendant_id: int
    ) -> Data:
        timestamp = datetime.now(timezone.utc).replace(microsecond=0)
        return Data(
            plaintiff_id=ctx.author.id,
            defendant_id=defendant_id,
            accusation=self.sanitize_text(data.get("accusation") or ""),
            facts=self.sanitize_text(data.get("facts") or ""),
            judges=set(),
            allowed_roles=set(),
            mute_list=set(),
            roles={},
            evidence_queue=[],
            created_at=timestamp,
            updated_at=timestamp,
        )

    @staticmethod
    async def create_new_chat_thread(
        channel: interactions.GuildText,
    ) -> interactions.ThreadChannel:
        try:
            return await channel.create_thread(
                name="Chat Room",
                thread_type=interactions.ChannelType.GUILD_PUBLIC_THREAD,
                auto_archive_duration=interactions.AutoArchiveDuration.ONE_WEEK,
                reason="Chat room for lawsuit management",
            )
        except HTTPException:
            logger.error("Chat thread creation failed")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error while creating chat thread: {e}")
            raise

    async def create_chat_room_setup_embed(
        self, thread: interactions.ThreadChannel
    ) -> interactions.Embed:
        try:
            thread_url = f"https://discord.com/channels/{thread.guild.id}/{thread.id}"
            return await self.view.create_embed(
                title="Success",
                description=f"Lawsuit and appeal buttons have been initialized. Chat room thread is now accessible: [Jump to Thread]({thread_url}).",
            )
        except Exception:
            logger.error("Chat room setup embed creation failed")
            raise

    async def create_trial_thread(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        is_public: bool,
    ) -> None:
        try:
            courtroom = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            thread_type = (
                interactions.ChannelType.GUILD_PUBLIC_THREAD
                if is_public
                else interactions.ChannelType.GUILD_PRIVATE_THREAD
            )
            trial_thread = await courtroom.create_thread(
                name=f"Case #{case_id} {'Public' if is_public else 'Private'} Trial Chamber",
                thread_type=thread_type,
                auto_archive_duration=interactions.AutoArchiveDuration.ONE_WEEK,
                reason=f"Trial thread for case {case_id}",
            )
            case.trial_thread_id = trial_thread.id
            case.allowed_roles = await self.get_allowed_members(ctx.guild)
            await trial_thread.send(
                embeds=[await self.view.create_summary_embed(case_id, case)],
                components=[self.view.create_trial_end_button(case_id)],
            )
            if is_public:
                await self.delete_thread_created_message(
                    self.config.COURTROOM_CHANNEL_ID, trial_thread
                )
            visibility_status = "publicly" if is_public else "privately"
            await self.view.send_success(
                ctx, f"Case #{case_id} will proceed {visibility_status}."
            )
            await self.notify_participants(
                case_id, f"Case #{case_id} will proceed {visibility_status}."
            )
            await self.add_members_to_thread(trial_thread, case.judges)
        except Exception as e:
            error_message = (
                "Trial thread creation failed"
                if isinstance(e, HTTPException)
                else "Unexpected error occurred during trial thread initialization"
            )
            logger.exception(error_message, extra={"case_id": case_id})
            await self.view.send_error(ctx, f"{error_message} for case {case_id}.")
            raise

    # Retrieval

    @staticmethod
    async def get_allowed_members(guild: interactions.Guild) -> set[int]:
        return {
            m.id
            for m in guild.members
            if any(r.name in {role.value for role in CaseRole} for r in m.roles)
        }

    async def get_judges(self, guild: interactions.Guild) -> set[int]:
        role = guild.get_role(self.config.JUDGE_ROLE_ID)
        return {m.id for m in role.members} if role else set()

    async def get_or_create_chat_thread(
        self, channel: interactions.GuildText
    ) -> interactions.ThreadChannel:
        return await self.find_existing_chat_thread(
            channel
        ) or await self.create_new_chat_thread(channel)

    @staticmethod
    async def find_existing_chat_thread(
        channel: interactions.GuildText,
    ) -> Optional[interactions.ThreadChannel]:
        threads = await channel.fetch_active_threads()
        if not threads:
            return None
        for thread in threads.threads:
            if thread.name == "Chat Room":
                return thread
        return None

    async def get_active_case_for_user(self, user_id: int) -> Optional[str]:
        cases = await self.repo.get_all_cases()
        return next(
            (
                case_id
                for case_id, case in cases.items()
                if user_id in {case.med_thread_id, case.defendant_id}
                and case.status != CaseStatus.CLOSED
            ),
            None,
        )

    async def find_case_number_by_channel_id(self, channel_id: int) -> Optional[str]:
        cases = await self.repo.get_all_cases()
        return next(
            (
                case_id
                for case_id, case in cases.items()
                if channel_id in {case.med_thread_id, case.trial_thread_id}
            ),
            None,
        )

    # Concurrency Synchronization

    @staticmethod
    async def cancel_task(task: asyncio.Task) -> None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def get_or_create_lock(self, case_id: str) -> asyncio.Lock:
        async with self.lock_creation_lock:
            return self.case_locks.setdefault(case_id, asyncio.Lock())

    @contextlib.asynccontextmanager
    async def acquire_case_lock(
        self, case_id: str
    ) -> AsyncGenerator[Optional[Data], None]:
        try:
            async with asyncio.timeout(5.0), await self.get_or_create_lock(case_id):
                case = await self.repo.get_case(case_id)
                yield case
                if case is not None and case.status == CaseStatus.CLOSED:
                    self.case_locks.pop(case_id, None)
        except asyncio.TimeoutError:
            logger.error(
                "Failed to acquire lock within timeout period",
                extra={"case_id": case_id},
            )
            raise
        except Exception as e:
            logger.exception(
                f"Encountered error during lock operation: {type(e).__name__}",
                extra={"case_id": case_id},
            )
            raise

    # Transcript

    async def enqueue_case_file_save(self, case_id: str, case: Data) -> None:
        async def save_case_file() -> None:
            try:
                log_channel = await self.bot.fetch_channel(self.config.LOG_CHANNEL_ID)
                log_forum = await self.bot.fetch_channel(self.config.LOG_FORUM_ID)
                log_post = await log_forum.fetch_message(self.config.LOG_POST_ID)

                timestamp = int(time.time())
                file_name = f"case_{case_id}_{timestamp}.txt"
                content = await self.generate_transcript(case_id, case)
                msg = f"Case {case_id} archived at {datetime.now(timezone.utc).isoformat()}"

                buffer = io.BytesIO(content.encode())
                file = interactions.File(buffer, file_name)

                await asyncio.gather(
                    *(
                        (
                            dest.send(msg, files=[file])
                            if isinstance(
                                dest,
                                (interactions.GuildText, interactions.GuildForumPost),
                            )
                            else dest.reply(msg, files={"files": [file]})
                        )
                        for dest in (log_channel, log_post)
                    )
                )
                buffer.close()

            except Exception:
                logger.exception(f"Failed to archive case {case_id}")
                raise

        asyncio.create_task(self.case_task_queue.put(save_case_file))

    async def generate_transcript(self, case_id: str, case: Data) -> str:
        metadata = {
            "Case ID": case_id,
            "Plaintiff ID": getattr(case, "plaintiff_id", None),
            "Defendant ID": getattr(case, "defendant_id", None),
            "Allegation": getattr(case, "accusation", "") or "",
            "Supporting Evidence": getattr(case, "facts", "") or "",
            "Filing Timestamp": datetime.now(timezone.utc).isoformat(),
            "Status": getattr(getattr(case, "status", None), "name", "Unknown"),
        }

        header = "".join(f"- **{k}:** {v}\n" for k, v in metadata.items()) + "\n"

        thread_logs = []
        for thread_id in filter(None, (case.med_thread_id, case.trial_thread_id)):
            if channel := await self.bot.fetch_channel(thread_id):
                if messages := await channel.history(limit=0).flatten():
                    thread_content = [f"\n## {channel.name}\n"]
                    thread_content.extend(
                        "\n".join(
                            filter(
                                None,
                                (
                                    f"{msg.author} at {msg.created_at.isoformat()}: {msg.content}",
                                    (
                                        f"Attachments: {','.join(a.url for a in msg.attachments)}"
                                        if msg.attachments
                                        else None
                                    ),
                                    (
                                        f"Modified at {msg.edited_timestamp.isoformat() if msg.edited_timestamp else 'Unknown'}"
                                        if msg.edited_timestamp
                                        else None
                                    ),
                                ),
                            )
                        )
                        for msg in reversed(messages)
                    )
                    thread_logs.append("".join(thread_content))

        return header + "".join(thread_logs)
