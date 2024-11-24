from __future__ import annotations

import array
import asyncio
import collections
import contextlib
import dataclasses
import fcntl
import functools
import io
import logging
import multiprocessing
import os
import random
import re
import secrets
import time
import types
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from enum import IntEnum, StrEnum, auto
from logging.handlers import RotatingFileHandler
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Coroutine,
    Dict,
    FrozenSet,
    Generic,
    List,
    Optional,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
)

import aiofiles
import aiofiles.os
import aiohttp
import bleach
import cachetools
import interactions
import orjson
import pydantic
from interactions.api.events import ExtensionUnload, MessageCreate, NewThreadCreate
from interactions.client.errors import Forbidden, HTTPException, NotFound

T = TypeVar("T", bound=object)
P = TypeVar("P", bound=object, contravariant=True)
R = TypeVar("R", bound=object)

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


@dataclasses.dataclass
class Config:
    GUILD_ID: int = 1150630510696075404
    JUDGE_ROLE_ID: int = 1200100104682614884
    PLAINTIFF_ROLE_ID: int = 1200043628899356702
    COURTROOM_CHANNEL_ID: int = 1247290032881008701
    LOG_CHANNEL_ID: int = 1166627731916734504
    LOG_FORUM_ID: int = 1159097493875871784
    LOG_POST_ID: int = 1279118293936111707

    MAX_JUDGES_PER_LAWSUIT: int = 1
    MAX_JUDGES_PER_APPEAL: int = 3
    MAX_FILE_SIZE: int = 10 * 1024 * 1024
    ALLOWED_MIME_TYPES: FrozenSet[str] = frozenset(
        {"image/jpeg", "image/png", "application/pdf", "text/plain"}
    )


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


class CaseRole(StrEnum):
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


class RoleAction(StrEnum):
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


class Attachment(pydantic.BaseModel):
    url: str = pydantic.Field(frozen=True, repr=True)
    filename: str = pydantic.Field(
        min_length=1, max_length=255, frozen=True, pattern=r'^[^/\\:*?"<>|]+$'
    )
    content_type: str = pydantic.Field(
        min_length=1, max_length=255, frozen=True, pattern=r"^[\w-]+/[\w-]+$"
    )


class Evidence(pydantic.BaseModel):
    user_id: int = pydantic.Field(gt=0, frozen=True)
    content: str = pydantic.Field(min_length=1, max_length=4000, frozen=True)
    attachments: Tuple[Attachment, ...] = pydantic.Field(
        default_factory=tuple,
        max_length=10,
        frozen=True,
    )
    message_id: int = pydantic.Field(gt=0, frozen=True)
    timestamp: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0),
        frozen=True,
    )
    state: EvidenceAction = pydantic.Field(default=EvidenceAction.PENDING)


class Data(pydantic.BaseModel):
    plaintiff_id: int = pydantic.Field(gt=0, frozen=True)
    defendant_id: int = pydantic.Field(gt=0, frozen=True)
    status: CaseStatus = pydantic.Field(default=CaseStatus.FILED)
    thread_id: Optional[int] = pydantic.Field(default=None, gt=0)
    judges: FrozenSet[int] = pydantic.Field(default_factory=frozenset)
    trial_thread_id: Optional[int] = pydantic.Field(default=None, gt=0)
    allowed_roles: FrozenSet[int] = pydantic.Field(default_factory=frozenset)
    log_message_id: Optional[int] = pydantic.Field(default=None, gt=0)
    mute_list: FrozenSet[int] = pydantic.Field(default_factory=frozenset)
    roles: Dict[str, FrozenSet[int]] = pydantic.Field(
        default_factory=lambda: types.MappingProxyType({}), frozen=True
    )
    evidence_queue: Tuple[Evidence, ...] = pydantic.Field(
        default_factory=tuple, max_length=100
    )
    accusation: Optional[str] = pydantic.Field(
        default=None, max_length=1000, min_length=1, pattern=r"^[\s\S]*$"
    )
    facts: Optional[str] = pydantic.Field(
        default=None, max_length=2000, min_length=1, pattern=r"^[\s\S]*$"
    )
    created_at: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0),
        frozen=True,
    )
    updated_at: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0),
        frozen=True,
    )


class Store:
    def __init__(self) -> None:
        self.db_path = os.path.join(BASE_DIR, "cases.json")
        self.store_initialized = False
        self.store_initialization_lock = asyncio.Lock()

    async def initialize_store(self) -> None:
        if self.store_initialized or self.store_initialization_lock.locked():
            return

        async with self.store_initialization_lock:
            if self.store_initialized:
                return

            try:
                await asyncio.to_thread(
                    os.makedirs, os.path.dirname(self.db_path), 0o755, True
                )
                if not await asyncio.to_thread(os.path.exists, self.db_path):
                    async with aiofiles.open(self.db_path, "wb") as f:
                        await f.write(b"{}")
                        await f.flush()
                        await asyncio.to_thread(os.fsync, f.fileno())
                self.store_initialized = True
            except OSError as e:
                logger.critical("Failed to initialize store: %s", repr(e))
                raise RuntimeError(
                    f"Store initialization failed: {e.__class__.__name__}"
                ) from e

    @contextlib.asynccontextmanager
    async def file_lock(self, mode: str, shared: bool = False) -> AsyncIterator[Any]:
        if not self.store_initialized:
            await self.initialize_store()

        async with aiofiles.open(self.db_path, mode=mode) as f:
            try:
                await asyncio.to_thread(
                    fcntl.flock,
                    f.fileno(),
                    (
                        fcntl.LOCK_SH | fcntl.LOCK_NB
                        if shared
                        else fcntl.LOCK_EX | fcntl.LOCK_NB
                    ),
                )
                yield f
                await asyncio.to_thread(fcntl.flock, f.fileno(), fcntl.LOCK_UN)
            except:
                raise

    async def read_all(self) -> Dict[str, Dict[str, Any]]:
        async with self.file_lock("rb", shared=True) as f:
            return orjson.loads(await f.read() or b"{}")

    async def read_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        async with self.file_lock("rb", shared=True) as f:
            return orjson.loads(await f.read() or b"{}").get(case_id)

    async def write_case(self, case_id: str, case_data: Dict[str, Any]) -> None:
        async with self.file_lock("r+b") as f:
            data = orjson.loads(await f.read() or b"{}")
            data[case_id] = case_data
            serialized = orjson.dumps(
                data,
                option=orjson.OPT_INDENT_2
                | orjson.OPT_SORT_KEYS
                | orjson.OPT_SERIALIZE_NUMPY,
                default=lambda x: None,
            )
            await f.seek(0)
            await f.write(serialized)
            await f.truncate()
            await f.flush()
            os.fsync(f.fileno())

    async def delete_case(self, case_id: str) -> None:
        async with self.file_lock("r+b") as f:
            data = orjson.loads(await f.read() or b"{}")
            if case_id in data:
                del data[case_id]
                serialized = orjson.dumps(
                    data,
                    option=orjson.OPT_INDENT_2
                    | orjson.OPT_SORT_KEYS
                    | orjson.OPT_SERIALIZE_NUMPY,
                )
                await f.seek(0)
                await f.write(serialized)
                await f.truncate()
                await f.flush()
                os.fsync(f.fileno())


class Repo(Generic[T]):
    def __init__(self) -> None:
        self.store = Store()
        self.cached_cases = cachetools.TTLCache(maxsize=1024, ttl=3600)
        self.repo_case_locks: collections.defaultdict[str, asyncio.Lock] = (
            collections.defaultdict(asyncio.Lock)
        )
        self.repo_initialization_lock = asyncio.Lock()
        self.repo_initialized = False

    async def initialize_repo(self) -> None:
        if self.repo_initialized or self.repo_initialization_lock.locked():
            return

        async with asyncio.timeout(10), self.repo_initialization_lock:
            if self.repo_initialized:
                return
            await self.store.initialize_store()
            self.repo_initialized = True

    @contextlib.asynccontextmanager
    async def case_lock_manager(self, case_id: str) -> AsyncIterator[None]:
        async with asyncio.timeout(5.0), self.repo_case_locks[case_id]:
            yield None

    async def get_case_data(self, case_id: str) -> Optional[T]:
        if not self.repo_initialized:
            await self.initialize_repo()

        if cached := self.cached_cases.get(case_id):
            return cached

        if case_data := await self.store.read_case(case_id):
            instance = Data(**dict(sorted(case_data.items())))
            self.cached_cases[case_id] = instance
            return instance
        return None

    async def persist_case(self, case_id: str, case: T) -> None:
        async with self.case_lock_manager(case_id):
            await self.store.write_case(case_id, case.__dict__)
            self.cached_cases[case_id] = case

    async def update_case(self, case_id: str, **kwargs: Any) -> None:
        async with self.case_lock_manager(case_id):
            if case := await self.get_case_data(case_id):
                updated = Data(
                    **(
                        dict(case.__dict__)
                        | {k: v for k, v in kwargs.items() if v is not None}
                    )
                )
                await self.persist_case(case_id, updated)
            else:
                raise KeyError(f"Case {case_id} not found")

    async def delete_case(self, case_id: str) -> None:
        async with self.case_lock_manager(case_id):
            await self.store.delete_case(case_id)
            self.cached_cases.pop(case_id, None)
            self.repo_case_locks.pop(case_id, None)

    async def get_all_cases(self) -> Dict[str, T]:
        if not self.repo_initialized:
            await self.initialize_repo()
        cases = {}
        for case_id, data in (await self.store.read_all()).items():
            try:
                cases[case_id] = self.cached_cases.setdefault(
                    case_id, Data(**dict(sorted(data.items())))
                )
            except pydantic.ValidationError:
                continue
        return cases


# View


class View:

    PROVERBS: Tuple[str, ...] = (
        "Iuris prudentia est divinarum atque humanarum rerum notitia, iusti atque iniusti scientia (D. 1, 1, 10, 2).",
        "Iuri operam daturum prius nosse oportet, unde nomen iuris descendat. est autem a iustitia appellatum: nam, ut eleganter celsus definit, ius est ars boni et aequi (D. 1, 1, 1, pr.).",
        "Iustitia est constans et perpetua voluntas ius suum cuique tribuendi (D. 1, 1, 10, pr.).",
    )

    def __init__(self, bot: interactions.Client) -> None:
        self.bot = bot
        self.config = Config()
        self.embed_lock = asyncio.Lock()
        self.button_lock = asyncio.Lock()

    async def generate_embed(
        self,
        title: str,
        description: str = "",
        color: EmbedColor = EmbedColor.INFO,
        retry_attempts: int = 3,
    ) -> interactions.Embed:
        async with self.embed_lock:
            for attempt in range(retry_attempts):
                try:
                    guild = await self.bot.fetch_guild(self.config.GUILD_ID)
                    embed = interactions.Embed(
                        title=title, description=description, color=int(color.value)
                    )
                    embed.timestamp = datetime.now(timezone.utc)
                    embed.set_footer(
                        text=guild.name if guild.icon else "鍵政大舞台",
                        icon_url=str(guild.icon.url) if guild.icon else None,
                    )
                    return embed

                except HTTPException as e:
                    if attempt == retry_attempts - 1:
                        logger.error("Failed to create embed", exc_info=e)
                        raise
                    await asyncio.sleep(1 << attempt)
                except Exception as e:
                    logger.error("Critical embed error", exc_info=e)
                    raise

    async def deliver_response(
        self,
        ctx: interactions.InteractionContext,
        title: str,
        message: str,
        color: EmbedColor,
        retry_attempts: int = 3,
    ) -> None:
        embed = await self.generate_embed(title, message, color)

        for attempt in range(retry_attempts):
            try:
                async with asyncio.timeout(5.0):
                    await ctx.send(embed=embed, ephemeral=True)
                    return
            except NotFound:
                if attempt < retry_attempts - 1:
                    try:
                        async with asyncio.timeout(5.0):
                            await ctx.send(embed=embed, ephemeral=True)
                            return
                    except Exception:
                        if attempt == retry_attempts - 1:
                            raise
                else:
                    raise
            except (asyncio.TimeoutError, Exception):
                if attempt == retry_attempts - 1:
                    raise
                await asyncio.sleep(1 << attempt)

    async def send_error(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.deliver_response(ctx, "Error", message, EmbedColor.ERROR)

    async def send_success(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.deliver_response(ctx, "Success", message, EmbedColor.INFO)

    def create_lawsuit_modal(self, is_appeal: bool = False) -> interactions.Modal:
        fields = self.define_modal_fields(is_appeal)
        form_type = "appeal" if is_appeal else "lawsuit"
        modal = interactions.Modal(
            *[
                type(f)(
                    custom_id=f"{f.custom_id}_{i}",
                    label=f.label,
                    placeholder=f.placeholder,
                    required=f.required,
                    min_length=f.min_length,
                    max_length=f.max_length,
                )
                for i, f in enumerate(fields)
            ],
            title=f"{form_type.title()} Form",
            custom_id=f"{form_type}_form_modal",
        )
        return modal

    @staticmethod
    def define_modal_fields(is_appeal: bool) -> tuple:
        base = (
            (
                ("case_number", "Case Number", "Enter the original case number"),
                ("appeal_reason", "Reason for Appeal", "Enter the reason for appeal"),
            )
            if is_appeal
            else (
                ("defendant_id", "Defendant ID", "Enter the defendant's user ID"),
                ("accusation", "Accusation", "Enter the accusation"),
            )
        )

        return tuple(
            interactions.ShortText(
                custom_id=field_id,
                label=label,
                placeholder=ph,
                min_length=1,
                max_length=4000,
            )
            for field_id, label, ph in base
        ) + (
            interactions.ParagraphText(
                custom_id="facts",
                label="Facts",
                placeholder="Describe the relevant facts",
                min_length=1,
                max_length=4000,
            ),
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
        embed = await self.generate_embed(f"Case #{case_id}")
        fields = {
            "Presiding Judge": " ".join(f"<@{j}>" for j in case.judges)
            or "Not yet assigned",
            "Plaintiff": f"<@{case.plaintiff_id}>",
            "Defendant": f"<@{case.defendant_id}>",
            "Accusation": case.accusation or "None",
            "Facts": case.facts or "None",
        }
        embed.add_fields(
            *[
                interactions.EmbedField(name=k, value=v, inline=False)
                for k, v in fields.items()
            ]
        )
        return embed

    @staticmethod
    def create_action_buttons(case_id: str) -> List[interactions.ActionRow]:
        return [
            interactions.ActionRow(
                interactions.Button(
                    style=interactions.ButtonStyle.DANGER,
                    label="Dismiss",
                    custom_id=f"dismiss_{case_id}",
                ),
                interactions.Button(
                    style=interactions.ButtonStyle.SUCCESS,
                    label="Accept",
                    custom_id=f"accept_{case_id}",
                ),
                interactions.Button(
                    style=interactions.ButtonStyle.SECONDARY,
                    label="Withdraw",
                    custom_id=f"withdraw_{case_id}",
                ),
            )
        ]

    async def create_trial_privacy_buttons(
        self, case_id: str
    ) -> Tuple[interactions.Button, ...]:
        async with self.button_lock:
            buttons = [
                interactions.Button(
                    style=interactions.ButtonStyle.SUCCESS,
                    label="Yes",
                    custom_id=f"public_trial_yes_{case_id}",
                ),
                interactions.Button(
                    style=interactions.ButtonStyle.DANGER,
                    label="No",
                    custom_id=f"public_trial_no_{case_id}",
                ),
            ]
            return tuple(buttons)

    @staticmethod
    def create_end_trial_button(case_id: str) -> interactions.Button:
        return interactions.Button(
            style=interactions.ButtonStyle.DANGER,
            label="End Trial",
            custom_id=f"end_trial_{case_id}",
        )

    @staticmethod
    def create_user_management_buttons(user_id: int) -> Tuple[interactions.Button, ...]:
        return (
            interactions.Button(
                style=interactions.ButtonStyle.PRIMARY,
                label="Mute",
                custom_id=f"mute_{user_id}",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.SUCCESS,
                label="Unmute",
                custom_id=f"unmute_{user_id}",
            ),
        )

    @staticmethod
    def create_message_management_buttons(
        message_id: int,
    ) -> Tuple[interactions.Button, ...]:
        return (
            interactions.Button(
                style=interactions.ButtonStyle.PRIMARY,
                label="Pin",
                custom_id=f"pin_{message_id}",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.DANGER,
                label="Delete",
                custom_id=f"delete_{message_id}",
            ),
        )

    @staticmethod
    def create_evidence_action_buttons(
        case_id: str, user_id: int
    ) -> Tuple[interactions.Button, ...]:
        return (
            interactions.Button(
                style=interactions.ButtonStyle.SUCCESS,
                label="Make Public",
                custom_id=f"approve_evidence_{case_id}_{user_id}",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.DANGER,
                label="Keep Private",
                custom_id=f"reject_evidence_{case_id}_{user_id}",
            ),
        )

    def select_daily_proverb(self, current_date: Optional[date] = None) -> str:
        indices = array.array(
            "L",
            random.SystemRandom().sample(range(len(self.PROVERBS)), len(self.PROVERBS)),
        )
        mask = len(self.PROVERBS) - 1
        current = current_date or date.today()
        return self.PROVERBS[indices[current.toordinal() & mask]]


# Controller


async def user_is_judge(ctx: interactions.BaseContext) -> bool:
    return any(r.id == Config().JUDGE_ROLE_ID for r in ctx.author.roles)


class Lawsuit(interactions.Extension):
    def __init__(self, bot: interactions.Client) -> None:
        self.bot = bot
        self.config: Config = Config()
        self.store: Store = Store()
        self.repo: Repo = Repo()
        self.view: View = View(bot)
        self.case_lock = asyncio.Lock()
        self.case_task_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10000)
        self.shutdown_event = asyncio.Event()
        self.lawsuit_button_message_id = None
        self.case_handlers = {
            CaseAction.FILE: self.handle_file_case_request,
            CaseAction.WITHDRAW: lambda ctx, case_id, case, member: self.handle_case_closure(
                ctx, case_id, case, member, action=CaseAction.WITHDRAW
            ),
            CaseAction.CLOSE: lambda ctx, case_id, case, member: self.handle_case_closure(
                ctx, case_id, case, member, action=CaseAction.CLOSE
            ),
            CaseAction.DISMISS: lambda ctx, case_id, case, member: self.handle_case_closure(
                ctx, case_id, case, member, action=CaseAction.DISMISS
            ),
            CaseAction.REGISTER: self.handle_accept_case_request,
        }
        self.ext_initialized = False
        self.ext_tasks: list[asyncio.Task[Any]] = []

    # Tasks

    def handle_task_exception(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            logger.info(f"Task {task.get_name()} cancelled gracefully")
            return

        if exc := task.exception():
            if isinstance(exc, asyncio.CancelledError):
                logger.info(f"Task {task.get_name()} cancelled via CancelledError")
            else:
                logger.critical(
                    f"Critical task failure in {task.get_name()}", exc_info=exc
                )
                self.restart_task(task)

    def restart_task(self, task: asyncio.Task[Any]) -> None:
        task_name = task.get_name().lower()
        if task_method := getattr(self, f"_{task_name}", None):
            new_task = asyncio.create_task(task_method(), name=task.get_name())
            new_task.add_done_callback(self.handle_task_exception)
            logger.info(f"Task {task.get_name()} restarted successfully")
        else:
            logger.error(f"Task restart failed - method not found: {task_name}")

    async def fetch_cases(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                async with asyncio.timeout(30):
                    cases = await self.repo.get_all_cases()
                    logger.info("Cases fetched successfully")

                    sleep_duration = min(300, max(60, len(cases) // 2))

                    try:
                        await asyncio.sleep(sleep_duration)
                    except asyncio.CancelledError:
                        logger.info("Fetch cases task cancelled during sleep")
                        return

            except asyncio.TimeoutError:
                logger.warning("Case fetch operation timed out, retrying in 60s")
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    logger.info("Fetch cases task cancelled during timeout recovery")
                    return
            except asyncio.CancelledError:
                logger.info("Fetch cases task cancelled")
                return
            except Exception as e:
                logger.exception("Case fetch failed with error: %s", str(e))
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    logger.info("Fetch cases task cancelled during error recovery")
                    return

    async def process_task_queue(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                task_callable = await asyncio.wait_for(
                    self.case_task_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            if not task_callable:
                self.case_task_queue.task_done()
                continue

            try:
                async with asyncio.timeout(30.0):
                    await asyncio.shield(
                        asyncio.create_task(
                            task_callable(), name=f"task_{id(task_callable)}"
                        )
                    )
            except Exception:
                logger.exception("Task processing failed")
            finally:
                self.case_task_queue.task_done()

    async def daily_embed_update(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(86400)
                await asyncio.shield(self.update_lawsuit_button_embed())
            except asyncio.CancelledError:
                logger.info("Embed update cancelled")
                return
            except Exception:
                logger.error("Embed update failed", exc_info=True)
                await asyncio.sleep(60)

    async def update_lawsuit_button_embed(self) -> None:
        if not (button_id := self.lawsuit_button_message_id):
            logger.warning("Button message ID not set")
            return

        try:
            async with asyncio.timeout(5.0):
                channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                message = await channel.fetch_message(button_id)
                embed = await self.view.generate_embed(
                    title=self.view.select_daily_proverb(),
                    description="Click the button to file a lawsuit or appeal.",
                )
                await asyncio.shield(message.edit(embeds=[embed]))
                logger.info("Embed updated")
        except Exception:
            logger.exception("Embed update failed")

    # Listeners

    @interactions.listen(NewThreadCreate)
    async def on_thread_create(self, event: NewThreadCreate) -> None:
        if event.thread.parent_id != self.config.COURTROOM_CHANNEL_ID:
            return

        try:
            async with asyncio.timeout(3.0):
                await asyncio.shield(
                    self.delete_thread_created_message(
                        event.thread.parent_id, event.thread
                    )
                )
        except asyncio.TimeoutError:
            logger.warning(
                "Thread deletion timed out", extra={"thread_id": event.thread.id}
            )
        except Exception:
            logger.exception(
                "Thread deletion failed", extra={"thread_id": event.thread.id}
            )

    @interactions.listen(MessageCreate)
    async def on_message_create(self, event: MessageCreate) -> None:
        if event.message.author.id == self.bot.user.id:
            return

        try:
            is_dm = isinstance(event.message.channel, interactions.DMChannel)
            handler = (
                self.handle_direct_message if is_dm else self.handle_channel_message
            )
            await handler(event.message)
        except Exception:
            logger.exception(
                "Message processing failed", extra={"message_id": event.message.id}
            )
            raise

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self) -> None:
        try:
            self.shutdown_event.set()

            for task in self.ext_tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass

            if hasattr(self.bot, "_http"):
                await self.bot._http.close()

            self.ext_initialized = False
            self.ext_tasks.clear()

        except Exception as e:
            logger.exception(f"Error during extension cleanup: {e}")

    # Components

    @interactions.component_callback("lawsuit_button")
    async def initiate_lawsuit(self, ctx: interactions.ComponentContext) -> None:
        if await self.has_ongoing_lawsuit(ctx.author.id):
            return await self.view.send_error(
                ctx, "You already have an ongoing lawsuit."
            )
        await ctx.send_modal(self.view.create_lawsuit_modal())

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
            if case_id and case:
                if await self.setup_mediation_thread(case_id, case):
                    updated_case = await self.repo.get_case_data(case_id)
                    if updated_case and updated_case.thread_id:
                        await self.notify_judges(case_id, ctx.guild_id)
                        await self.view.send_success(ctx, f"Mediation room #{case_id} created.")
                    else:
                        await self.view.send_error(ctx, "Failed to setup mediation room.")
                else:
                    await self.view.send_error(ctx, "Failed to setup mediation room.")
            else:
                await self.view.send_error(ctx, "Failed to create case.")

        except Exception:
            logger.exception("Lawsuit submission failed")
            await self.view.send_error(ctx, "An error occurred.")

    @interactions.component_callback(re.compile(r"dismiss_\d+"))
    async def handle_dismiss(self, ctx: interactions.ComponentContext) -> None:
        case_id = ctx.custom_id[8:]
        await self.process_case_action(ctx, CaseAction.DISMISS, case_id)

    @interactions.component_callback(re.compile(r"accept_\d+"))
    async def handle_accept(self, ctx: interactions.ComponentContext) -> None:
        case_id = ctx.custom_id[7:]
        await self.process_case_action(ctx, CaseAction.REGISTER, case_id)

    @interactions.component_callback(re.compile(r"withdraw_\d+"))
    async def handle_withdraw(self, ctx: interactions.ComponentContext) -> None:
        case_id = ctx.custom_id[9:]
        await self.process_case_action(ctx, CaseAction.WITHDRAW, case_id)

    @interactions.component_callback(re.compile(r"public_trial_(yes|no)_\d+"))
    async def handle_trial_privacy(self, ctx: interactions.ComponentContext) -> None:
        match = re.match(r"public_trial_(yes|no)_(\d+)", ctx.custom_id)
        if not match:
            return await self.view.send_error(ctx, "Invalid trial privacy action.")

        is_public_str, case_id = match.groups()
        async with self.repo.case_lock_manager(case_id) as case:
            if not case:
                return await self.view.send_error(ctx, "Case not found.")

            await self.create_trial_thread(
                ctx, case_id, case, is_public_str.lower() == "yes"
            )

    @interactions.component_callback(re.compile(r"(mute|unmute)_\d+"))
    async def handle_user_mute(self, ctx: interactions.ComponentContext) -> None:
        match = re.match(r"(mute|unmute)_(\d+)", ctx.custom_id)
        if not match:
            return await self.view.send_error(ctx, "Invalid mute action.")

        action, user_id = match.groups()
        case_id = await self.validate_user_permissions(ctx.author.id, user_id)

        if case_id:
            await self.process_mute_action(
                ctx, str(case_id), int(user_id), action == "mute"
            )
        else:
            await self.view.send_error(ctx, "Associated case not found.")

    @interactions.component_callback(re.compile(r"(pin|delete)_\d+"))
    async def handle_message_action(self, ctx: interactions.ComponentContext) -> None:
        match = re.match(r"(pin|delete)_(\d+)", ctx.custom_id)
        if not match:
            return await self.view.send_error(ctx, "Invalid message action.")

        action, message_id = match.groups()
        case_id = await self.validate_user_permissions(ctx.author.id, message_id)

        if case_id:
            await self.process_message_action(
                ctx, str(case_id), int(message_id), MessageAction(action)
            )
        else:
            await self.view.send_error(ctx, "Associated case not found.")

    @interactions.component_callback(re.compile(r"(approve|reject)_evidence_\d+_\d+"))
    async def handle_evidence_action(self, ctx: interactions.ComponentContext) -> None:
        match = re.match(r"(approve|reject)_evidence_(\d+)_(\d+)", ctx.custom_id)
        if not match:
            return await self.view.send_error(ctx, "Invalid evidence action.")

        action, case_id, user_id = match.groups()
        await self.process_evidence_decision(
            ctx,
            EvidenceAction.APPROVED if action == "approve" else EvidenceAction.REJECTED,
            case_id,
            int(user_id),
        )

    @interactions.component_callback(re.compile(r"end_trial_\d+"))
    async def handle_end_trial(self, ctx: interactions.ComponentContext) -> None:
        case_id = ctx.custom_id[10:]
        await self.process_case_action(ctx, CaseAction.CLOSE, case_id)

    # Commands

    module_base: interactions.SlashCommand = interactions.SlashCommand(
        name="lawsuit",
        description="Lawsuit management system",
    )

    @module_base.subcommand("init", sub_cmd_description="Initialize the lawsuit system")
    @interactions.check(user_is_judge)
    async def module_base_init(self, ctx: interactions.SlashContext) -> None:
        if self.ext_initialized:
            return await self.view.send_success(ctx, "System already initialized.")

        try:
            await ctx.defer(ephemeral=True)
            await self.repo.initialize_repo()

            tasks = [
                ("_fetch_cases", self.fetch_cases),
                ("_daily_embed_update", self.daily_embed_update),
                ("_process_task_queue", self.process_task_queue),
            ]

            self.ext_tasks = [
                asyncio.create_task(coro(), name=name) for name, coro in tasks
            ]
            for task in self.ext_tasks:
                task.add_done_callback(self.handle_task_exception)

            self.ext_initialized = True
            await self.view.send_success(
                ctx, "Lawsuit system initialized successfully."
            )

        except Exception:
            await self.view.send_error(ctx, "Failed to initialize system.")
            raise

    @module_base.subcommand(
        "dispatch",
        sub_cmd_description="Deploy the lawsuit management interface",
    )
    @interactions.check(user_is_judge)
    async def deploy_lawsuit_button(self, ctx: interactions.SlashContext) -> None:
        await ctx.defer(ephemeral=True)
        try:
            embed = await self.view.generate_embed(
                title=self.view.select_daily_proverb(),
                description="Click below to file a lawsuit or appeal.",
            )

            message = await ctx.channel.send(
                embeds=[embed],
                components=[self.view.action_buttons_store],
                allowed_mentions=interactions.AllowedMentions.none(),
            )

            self.lawsuit_button_message_id = message.id
            await self.initialize_chat_thread(ctx)

            logger.info(
                "Lawsuit interface deployed",
                extra={
                    "user_id": ctx.author.id,
                    "channel_id": ctx.channel_id,
                    "message_id": message.id,
                },
            )

        except Exception:
            logger.exception("Failed to deploy interface")
            await self.view.send_error(ctx, "Failed to deploy interface.")
            raise

    @module_base.subcommand(
        "role",
        sub_cmd_description="Manage user roles within a case",
    )
    @interactions.slash_option(
        name="action",
        description="Role management action",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            interactions.SlashCommandChoice(
                name=str(a.name).title(), value=str(a.value)
            )
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
    async def manage_user_role(
        self,
        ctx: interactions.SlashContext,
        action: str,
        role: str,
        user: interactions.User,
    ) -> None:
        try:
            case_id = await self.find_case_number_by_channel_id(ctx.channel_id)
            if not case_id:
                await self.view.send_error(ctx, "No associated case found.")
            interaction_action = RoleAction(action.lower())
            await self.manage_case_role(
                ctx, str(case_id), interaction_action, role, user
            )

            action_str = (
                "assigned to"
                if interaction_action == RoleAction.ASSIGN
                else "revoked from"
            )
            logger.info(
                "Role management completed.",
                extra={
                    "action": action_str,
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
    async def message_management_menu(
        self, ctx: interactions.ContextMenuContext
    ) -> None:
        try:
            if not await user_is_judge(ctx):
                await self.view.send_error(ctx, "Only judges can manage messages.")
                return

            case_id = await self.find_case_number_by_channel_id(ctx.channel_id)
            if not case_id:
                await self.view.send_error(
                    ctx, "This command can only be used in case threads."
                )
                return

            components = [
                interactions.ActionRow(
                    *self.view.create_message_management_buttons(ctx.target.id)
                )
            ]
            await ctx.send("Select an action:", components=components, ephemeral=True)

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
    async def user_management_menu(self, ctx: interactions.ContextMenuContext) -> None:
        try:
            if not await user_is_judge(ctx):
                await self.view.send_error(ctx, "Only judges can manage users.")
                return

            case_id = await self.find_case_number_by_channel_id(ctx.channel_id)
            if not case_id:
                await self.view.send_error(
                    ctx, "This command can only be used in case threads."
                )
                return

            components = [
                interactions.ActionRow(
                    *self.view.create_user_management_buttons(ctx.target.id)
                )
            ]
            await ctx.send("Select an action:", components=components, ephemeral=True)

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

    # Services

    async def process_case_action(
        self,
        ctx: interactions.ComponentContext,
        action: CaseAction,
        case_id: str,
    ) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                case_task = tg.create_task(self.repo.get_case_data(case_id))
                guild_task = tg.create_task(self.bot.fetch_guild(self.config.GUILD_ID))
                case, guild = await case_task, await guild_task

            if not case:
                await self.view.send_error(ctx, "Case not found.")
                return

            member = await guild.fetch_member(ctx.author.id)
            handler = self.case_handlers.get(action)

            if not handler:
                await self.view.send_error(ctx, "Invalid action specified.")
                return

            async with asyncio.timeout(5.0):
                await handler(ctx, case_id, case, member)  # type: ignore

        except* (asyncio.TimeoutError, asyncio.CancelledError):
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
        except* Exception as eg:
            await self.view.send_error(
                ctx, f"An unexpected error occurred: {type(eg).__name__}"
            )

    async def handle_file_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        user: Union[interactions.Member, interactions.User],
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                if not await user_is_judge(interactions.BaseContext(user)):
                    await self.view.send_error(ctx, "Insufficient permissions.")
                    return
                await self.add_judge_to_case(ctx, case_id)

        except* (asyncio.TimeoutError, asyncio.CancelledError) as eg:
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
            raise eg.derive([eg.exceptions[0]])
        except* Exception as eg:
            await self.view.send_error(ctx, f"An error occurred: {type(eg).__name__}")
            raise eg.derive([eg.exceptions[0]])

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
                await self.view.send_error(ctx, "Insufficient permissions.")
                return

            action_name = action.value.capitalize()
            case_status = CaseStatus(action.value)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.update_case_status(case, case_status))
                tg.create_task(self.notify_case_closure(ctx, case_id, action_name))
                tg.create_task(self.enqueue_case_file_save(case_id, case))

        except* Exception as eg:
            await self.view.send_error(
                ctx, f"Failed to close the case: {type(eg).__name__}"
            )
            raise eg.derive([eg.exceptions[0]])

    async def update_case_status(self, case: Data, new_status: CaseStatus) -> None:
        setattr(case, "status", new_status)
        thread_ids = {
            tid
            for tid in (
                getattr(case, "thread_id", None),
                getattr(case, "trial_thread_id", None),
            )
            if tid
        }

        if thread_ids:
            async with asyncio.TaskGroup() as tg:
                for thread_id in thread_ids:
                    tg.create_task(self.archive_thread(thread_id))

    async def notify_case_closure(
        self, ctx: interactions.ComponentContext, case_id: str, action_name: str
    ) -> None:
        notification = f"Case number {case_id} has been {action_name} by {ctx.author.display_name}."

        try:
            async with asyncio.timeout(5.0):
                await asyncio.gather(
                    self.notify_participants(case_id, notification),
                    self.view.send_success(
                        ctx, f"Case number {case_id} successfully {action_name}."
                    ),
                )
        except* Exception:
            await self.view.send_error(ctx, "Failed to notify participants.")

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
                    await self.view.send_error(ctx, "Insufficient permissions.")
                    return

                embed = await self.view.generate_embed(title="Public or Private Trial")
                buttons = self.view.create_trial_privacy_buttons(case_id)
                await ctx.send(
                    embeds=[embed],
                    components=[
                        interactions.ActionRow(*[button for button in await buttons])
                    ],
                    allowed_mentions=interactions.AllowedMentions.none(),
                )

        except* Exception:
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
        except* Exception:
            await self.view.send_error(ctx, "Failed to end trial.")

    # Threads

    @staticmethod
    async def add_members_to_thread(
        thread: interactions.ThreadChannel, member_ids: FrozenSet[int]
    ) -> None:
        async def _add_member(member_id: int) -> None:
            try:
                async with asyncio.timeout(5.0):
                    await thread.add_member(await thread.guild.fetch_member(member_id))
                    logger.debug("Added member", extra={"member_id": member_id})
            except Exception:
                logger.error(
                    "Failed to add member",
                    extra={"member_id": member_id},
                    exc_info=True,
                )

        async with asyncio.TaskGroup() as tg:
            for mid in member_ids:
                tg.create_task(_add_member(mid))

    async def add_judge_to_case(
        self, ctx: interactions.ComponentContext, case_id: str
    ) -> None:
        try:
            async with self.repo.case_lock_manager(case_id) as case:
                if not case:
                    return await self.view.send_error(ctx, "Case not found.")

                if not hasattr(case, "judges"):
                    return await self.view.send_error(ctx, "Invalid case data.")

                judges = getattr(case, "judges", frozenset())
                if len(judges) >= self.config.MAX_JUDGES_PER_LAWSUIT:
                    return await self.view.send_error(ctx, "Maximum judges reached.")

                if ctx.author.id in judges:
                    return await self.view.send_error(ctx, "Already a judge.")

                new_judges = frozenset({*judges, ctx.author.id})
                notification = (
                    f"{ctx.author.display_name} assigned as judge to Case #{case_id}."
                )

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.repo.update_case(case_id, judges=new_judges))
                    tg.create_task(self.notify_participants(case_id, notification))
                    tg.create_task(
                        self.update_thread_permissions(case_id, ctx.author.id)
                    )
                    tg.create_task(
                        self.view.send_success(
                            ctx, f"Added as judge to Case #{case_id}."
                        )
                    )

        except Exception:
            logger.error(
                "Failed to add judge", extra={"case_id": case_id}, exc_info=True
            )
            await self.view.send_error(ctx, "Failed to add judge.")
            raise

    async def update_thread_permissions(self, case_id: str, judge_id: int) -> None:
        try:
            async with asyncio.timeout(5.0):
                case = await self.repo.get_case_data(case_id)
                thread_ids = {
                    tid
                    for tid in ("thread_id", "trial_thread_id")
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

                async with asyncio.TaskGroup() as tg:
                    for tid in thread_ids:
                        channel = await self.bot.fetch_channel(tid)
                        tg.create_task(channel.edit_permission(perms))

        except Exception:
            logger.error(
                "Failed to update permissions",
                extra={"case_id": case_id},
                exc_info=True,
            )
            raise

    # Users

    async def process_mute_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        mute: bool,
    ) -> None:
        async with self.repo.case_lock_manager(case_id):
            case = await self.repo.get_case_data(case_id)
            if not case:
                raise ValueError(f"Case {case_id} not found")

            mute_list: frozenset[int] = getattr(case, "mute_list", frozenset())
            action_str = "muted" if mute else "unmuted"

            if (target_id in mute_list) == mute:
                await self.view.send_error(
                    ctx, f"User <@{target_id}> is already {action_str}."
                )
                return

            new_mute_list = mute_list | {target_id} if mute else mute_list - {target_id}
            notification = (
                f"User <@{target_id}> has been {action_str} by <@{ctx.author.id}>."
            )

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self.repo.update_case(
                        case_id, mute_list=tuple(sorted(new_mute_list))
                    )
                )
                tg.create_task(self.view.send_success(ctx, notification))
                tg.create_task(self.notify_participants(case_id, notification))

    async def process_message_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        action_type: MessageAction,
    ) -> None:
        try:
            channel = await self.bot.fetch_channel(ctx.channel_id)
            if not channel:
                raise ValueError(f"Channel {ctx.channel_id} not found")

            message = await channel.fetch_message(target_id)
            if not message:
                raise ValueError(f"Message {target_id} not found")

            action_map = {
                MessageAction.PIN: (message.pin, "pinned"),
                MessageAction.DELETE: (message.delete, "deleted"),
            }

            action_func, action_str = action_map[action_type]
            await action_func()

            notification = f"A message has been {action_str} by <@{ctx.author.id}> in case {case_id}."
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self.view.send_success(ctx, f"Message has been {action_str}.")
                )
                tg.create_task(self.notify_participants(case_id, notification))

        except NotFound:
            await self.view.send_error(ctx, f"Message not found in case {case_id}.")
        except Forbidden:
            await self.view.send_error(
                ctx, f"Insufficient permissions to {action_type} the message."
            )
        except HTTPException as e:
            await self.view.send_error(
                ctx, f"Failed to {action_type} message: {str(e)}"
            )

    # Notification

    async def notify_evidence(self, user_id: int, case_id: str, decision: str) -> None:
        embed = await self.view.generate_embed(
            title="Evidence Decision",
            description=f"Your evidence for **Case {case_id}** has been **{decision}**.",
        )
        await self.send_dm(user_id, embed=embed)

    async def notify_participants(self, case_id: str, message: str) -> None:
        case = await self.repo.get_case_data(case_id)
        if not case:
            logger.warning(
                "Case %s not found when attempting to notify participants", case_id
            )
            return

        participants = frozenset(
            {getattr(case, "plaintiff_id", None), getattr(case, "defendant_id", None)}
            | getattr(case, "judges", frozenset())
        )

        embed = await self.view.generate_embed(
            title=f"Case {case_id} Update", description=message
        )
        await self.notify_users(
            user_ids=frozenset(filter(None, participants)),
            notification_func=functools.partial(self.send_dm, embed=embed),
        )

    async def notify_judges_for_evidence(self, case_id: str, user_id: int) -> None:
        case = await self.repo.get_case_data(case_id)
        if not case:
            logger.warning("Case %s not found for evidence notification", case_id)
            return

        embed = await self.view.generate_embed(
            title="Evidence Submission",
            description=f"New evidence has been submitted for **Case {case_id}**. Please review and decide whether to make it public.",
        )
        buttons = self.view.create_evidence_action_buttons(case_id, user_id)
        await self.notify_users(
            user_ids=getattr(case, "judges", frozenset()),
            notification_func=functools.partial(
                self.send_dm,
                embed=embed,
                components=[interactions.ActionRow(*buttons)],
            ),
        )

    async def notify_judges(self, case_id: str, guild_id: int) -> None:
        async with asyncio.TaskGroup() as tg:
            guild = await tg.create_task(self.bot.fetch_guild(guild_id))
            case = await tg.create_task(self.repo.get_case_data(case_id))

        if not case or not getattr(case, "thread_id", None):
            logger.warning("Case %s missing thread for notifications", case_id)
            return

        judge_members = frozenset(
            m.id
            for m in guild.members
            if self.config.JUDGE_ROLE_ID in {r.id for r in m.roles}
        )
        if not judge_members:
            logger.warning("No judges found for case %s", case_id)
            return

        thread_link = f"https://discord.com/channels/{guild_id}/{case.thread_id}"
        embed = await self.view.generate_embed(
            title=f"Case #{case_id} Complaint Received",
            description=f"Please proceed to the [mediation room]({thread_link}) to participate in mediation.",
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
            user_ids=judge_members,
            notification_func=functools.partial(
                self.send_dm, embed=embed, components=components
            ),
        )

    @staticmethod
    async def notify_users(
        user_ids: frozenset[int],
        notification_func: Callable[[int], Coroutine[Any, Any, None]],
    ) -> None:
        if not user_ids:
            return

        async with asyncio.TaskGroup() as tg:
            tasks: list[asyncio.Task[None]] = [
                tg.create_task(notification_func(uid)) for uid in user_ids
            ]
            await tasks[0]
            if len(tasks) > 1:
                await asyncio.gather(*tasks[1:])

    async def send_dm(self, user_id: int, **kwargs: Any) -> None:
        try:
            async with asyncio.timeout(5.0):
                user = await self.bot.fetch_user(user_id)
                await user.send(**kwargs)
                logger.debug("DM sent", extra={"user_id": user_id})
        except Exception as e:
            level = (
                logging.WARNING
                if isinstance(e, (asyncio.TimeoutError, HTTPException))
                else logging.ERROR
            )
            logger.log(level, "DM failed", extra={"user_id": user_id}, exc_info=True)

    # Roles

    @manage_user_role.autocomplete("role")
    async def role_autocomplete(self, ctx: interactions.AutocompleteContext) -> None:
        input_text = ctx.input_text.lower()
        choices = [
            {"name": role.value, "value": role.name}
            for role in CaseRole
            if input_text in role.value.lower()
        ][:25]
        await ctx.send(choices)

    async def manage_case_role(
        self,
        ctx: interactions.SlashContext,
        case_id: str,
        action: RoleAction,
        role: str,
        user: interactions.User,
    ) -> None:
        try:
            async with asyncio.timeout(5.0), self.case_lock_manager(case_id):
                case_data = await self.repo.get_case_data(case_id)
                if not case_data:
                    raise ValueError(f"Case `{case_id}` not found.")

                case = Data.model_validate(case_data)
                if not await self.is_user_assigned_to_case(
                    ctx.author.id, case_id, "judges"
                ):
                    raise PermissionError(
                        "Insufficient permissions to manage case roles."
                    )

                try:
                    role_enum = CaseRole[role.upper()]
                except KeyError:
                    raise ValueError(f"Invalid role: `{role}`.")

                roles = {k: set(v) for k, v in case.roles.items()}
                users_with_role = roles.setdefault(role_enum.name, set())

                if action == RoleAction.ASSIGN:
                    if user.id in users_with_role:
                        raise ValueError(
                            f"User <@{user.id}> already has the role `{role_enum.name}`."
                        )
                    users_with_role.add(user.id)
                    action_str, prep = "Assigned", "to"
                else:
                    if user.id not in users_with_role:
                        raise ValueError(
                            f"User <@{user.id}> does not possess the role `{role_enum.name}`."
                        )
                    users_with_role.remove(user.id)
                    action_str, prep = "Revoked", "from"

                await self.repo.update_case(case_id, roles=roles)

                notification = f"<@{ctx.author.id}> has {action_str.lower()} the role of `{role_enum.name}` {prep} <@{user.id}>."
                success_msg = f"Successfully {action_str.lower()} role `{role_enum.name}` {prep} <@{user.id}>."

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.view.send_success(ctx, success_msg))
                    tg.create_task(self.notify_participants(case_id, notification))

        except (ValueError, PermissionError) as e:
            logger.warning("Role management error in case '%s': %s", case_id, e)
            await self.view.send_error(ctx, str(e))
        except asyncio.TimeoutError:
            logger.error("Timeout acquiring lock for case '%s'", case_id)
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
        except Exception as e:
            logger.exception(
                "Unexpected error in manage_case_role for case '%s': %s", case_id, e
            )
            await self.view.send_error(
                ctx, "An unexpected error occurred while processing your request."
            )

    # Evidence

    async def process_evidence_decision(
        self,
        ctx: interactions.ComponentContext,
        action: EvidenceAction,
        case_id: str,
        user_id: int,
    ) -> None:
        async with self.case_lock_manager(case_id) as case:
            if not case:
                await self.view.send_error(ctx, "Case not found.")
                return

            evidence_queue = getattr(case, "evidence_queue", [])
            evidence = next((e for e in evidence_queue if e.user_id == user_id), None)
            if not evidence:
                await self.view.send_error(ctx, "Evidence not found.")
                return

            setattr(evidence, "state", action)
            evidence_list = f"evidence_{action.name.lower()}"
            setattr(case, evidence_list, [*getattr(case, evidence_list, []), evidence])
            setattr(
                case,
                "evidence_queue",
                [e for e in evidence_queue if e.user_id != user_id],
            )

            await self.repo.update_case(
                case_id,
                evidence_queue=getattr(case, "evidence_queue"),
                **{evidence_list: getattr(case, evidence_list)},
            )

            notification = f"<@{user_id}>'s evidence has been {action.name.lower()} by <@{ctx.author.id}>."
            await self.notify_participants(case_id, notification)
            await self.notify_evidence(user_id, case_id, action.name.lower())
            await self.handle_evidence_finalization(ctx, case, evidence, action)
            await self.view.send_success(
                ctx, f"Evidence {action.name.lower()} successfully."
            )

    async def handle_evidence_finalization(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
        new_state: EvidenceAction,
    ) -> None:
        if new_state is EvidenceAction.APPROVED:
            await self.publish_approved_evidence(ctx, case, evidence)
        else:
            await self.handle_rejected_evidence(ctx, evidence)

    async def publish_approved_evidence(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
    ) -> None:
        if not case.trial_thread_id:
            await self.view.send_success(ctx, "Evidence approved and queued for trial.")
            return

        try:
            trial_thread = await self.bot.fetch_channel(case.trial_thread_id)
            content = f"Evidence from <@{evidence.user_id}>:\n{evidence.content}"

            files = []
            for att in evidence.attachments:
                if file := await self.fetch_attachment_as_file(
                    {"url": att.url, "filename": att.filename}
                ):
                    files.append(file)

            await trial_thread.send(content=content, files=files)
            await self.view.send_success(ctx, "Evidence published to trial thread.")
        except Exception as e:
            logger.error(f"Failed to publish evidence: {e}")
            await self.view.send_error(ctx, "Failed to publish evidence.")

    @staticmethod
    async def fetch_attachment_as_file(
        attachment: Dict[str, str]
    ) -> Optional[interactions.File]:
        url, filename = attachment["url"], attachment["filename"]
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if not response.ok:
                        logger.warning(
                            f"Failed to fetch attachment: HTTP {response.status}",
                            extra={"url": url},
                        )
                        return None
                    content = await response.read()
                    if not content:
                        return None
                    file = io.BytesIO(content)
                    file.seek(0)
                    return interactions.File(
                        file=file,
                        file_name=filename,
                        description=f"Evidence attachment for {filename}",
                    )
        except Exception as e:
            logger.error(f"Error fetching attachment: {e}", extra={"url": url})
            return None

    async def handle_rejected_evidence(
        self,
        ctx: interactions.ComponentContext,
        evidence: Evidence,
    ) -> None:
        try:
            user = await self.bot.fetch_user(evidence.user_id)
            await user.send("Your evidence has been rejected.")
            await self.view.send_success(ctx, "Evidence rejection processed.")
        except Exception as e:
            logger.error(f"Failed to handle rejected evidence: {e}")
            await self.view.send_error(ctx, "Failed to process evidence rejection.")

    async def handle_direct_message(
        self,
        message: interactions.Message,
    ) -> None:
        try:
            case_id = await self.get_user_active_case_number(message.author.id)
            if not case_id:
                await message.author.send("No active cases found.")
                return

            evidence = self.create_evidence_dict(message, self.config.MAX_FILE_SIZE)
            await self.process_evidence(case_id, evidence)
            await message.author.send("Evidence processed successfully.")
        except Exception as e:
            logger.error(f"Failed to handle evidence: {e}")
            await message.author.send("Failed to process evidence.")

    async def handle_channel_message(
        self,
        message: interactions.Message,
    ) -> None:
        case_id = await self.find_case_number_by_channel_id(message.channel.id)
        if not case_id:
            return

        try:
            case = await self.repo.get_case_data(case_id)
            if case and message.author.id in case.mute_list:
                await message.delete()
        except Exception as e:
            logger.error(f"Failed to handle channel message: {e}")

    def create_evidence_dict(
        self,
        message: interactions.Message,
        max_file_size: int,
    ) -> Evidence:
        return Evidence(
            user_id=message.author.id,
            content=self.sanitize_user_input(message.content),
            attachments=tuple(
                Attachment(url=a.url, filename=a.filename, content_type=a.content_type)
                for a in message.attachments
                if self.is_valid_attachment(a, max_file_size)
            ),
            message_id=message.id,
            timestamp=message.created_at,
            state=EvidenceAction.PENDING,
        )

    async def process_evidence(
        self,
        case_id: str,
        evidence: Evidence,
    ) -> None:
        async with self.case_lock_manager(case_id) as case:
            if not case or case.status not in {
                CaseStatus.FILED,
                CaseStatus.IN_PROGRESS,
            }:
                raise ValueError("Case not accepting evidence.")

            case.evidence_queue = [*case.evidence_queue, evidence]
            await self.repo.update_case(case_id, evidence_queue=case.evidence_queue)
            await self.notify_judges_for_evidence(case_id, evidence.user_id)

    # Validation

    async def user_has_judge_role(
        self, user: Union[interactions.Member, interactions.User]
    ) -> bool:
        member = (
            user
            if isinstance(user, interactions.Member)
            else await (await self.bot.fetch_guild(self.config.GUILD_ID)).fetch_member(
                user.id
            )
        )
        return self.config.JUDGE_ROLE_ID in [role.id for role in member.roles]

    async def user_has_permission_for_closure(
        self, user_id: int, case_id: str, member: interactions.Member
    ) -> bool:
        return await self.is_user_assigned_to_case(
            user_id, case_id, "plaintiff_id"
        ) and self.config.JUDGE_ROLE_ID in [r.id for r in member.roles]

    def is_valid_attachment(
        self, attachment: interactions.Attachment, max_size: int
    ) -> bool:
        if not attachment.content_type.startswith("image/"):
            return True
        return (
            attachment.content_type in self.config.ALLOWED_MIME_TYPES
            and attachment.size <= max_size
            and attachment.filename
            and attachment.height is not None
        )

    @staticmethod
    def get_role_check(role: str) -> Callable[[Data, int], bool]:
        if role == "plaintiff_id":
            return lambda c, u: c.plaintiff_id == u
        if role == "defendant_id":
            return lambda c, u: c.defendant_id == u
        if role == "judges":
            return lambda c, u: u in c.judges
        if role == "witnesses":
            return lambda c, u: u in c.witnesses
        if role == "attorneys":
            return lambda c, u: u in c.attorneys
        return lambda c, u: u in getattr(c, role, frozenset())

    async def is_user_assigned_to_case(
        self, user_id: int, case_id: str, role: str
    ) -> bool:
        try:
            case = await self.repo.get_case_data(case_id)
            return bool(self.get_role_check(role)(Data.model_validate(case), user_id))
        except Exception:
            logger.exception(
                "Assignment check failed", extra={"user": user_id, "case": case_id}
            )
            return False

    async def has_ongoing_lawsuit(self, user_id: int) -> bool:
        cases = await self.repo.get_all_cases()
        return any(
            Data.model_validate(case).plaintiff_id == user_id
            and Data.model_validate(case).status != CaseStatus.CLOSED
            for case in cases.values()
        )

    async def validate_user_permissions(self, user_id: int, case_id: str) -> bool:
        try:
            case = await self.repo.get_case_data(case_id)
            if not case:
                return False
            case_data = Data.model_validate(case)
            return user_id in case_data.judges or user_id in {
                case_data.plaintiff_id,
                case_data.defendant_id,
            }
        except Exception:
            logger.exception(
                "Permission check failed", extra={"user": user_id, "case": case_id}
            )
            return False

    # Helper

    async def archive_thread(self, thread_id: int) -> bool:
        try:
            thread = await self.bot.fetch_channel(thread_id)
            if not isinstance(thread, interactions.ThreadChannel):
                return False
            await thread.edit(archived=True, locked=True)
            return True
        except Exception:
            logger.exception("Failed to archive thread", extra={"thread_id": thread_id})
            return False

    @staticmethod
    def sanitize_user_input(text: str, *, max_length: int = 2000) -> str:
        cleaner = functools.partial(
            bleach.clean,
            tags=frozenset(),
            attributes={},
            strip=True,
            protocols=bleach.ALLOWED_PROTOCOLS,
        )
        cleaned = cleaner(text)
        normalized = re.sub(r"\s+", " ", cleaned).strip()
        return normalized[:max_length]

    async def delete_thread_created_message(
        self,
        channel_id: int,
        thread: interactions.ThreadChannel,
    ) -> bool:
        try:
            channel = await self.bot.fetch_channel(channel_id)
            if not isinstance(channel, interactions.GuildText):
                return False

            async with asyncio.timeout(2.0):
                async for message in channel.history(limit=5):
                    if (
                        message.type == interactions.MessageType.THREAD_CREATED
                        and message.thread.id == thread.id
                    ):
                        try:
                            await message.delete()
                            return True
                        except Exception:
                            logger.warning(
                                "Failed to delete message",
                                extra={"message_id": message.id},
                                exc_info=True,
                            )
                            return False
            return False
        except asyncio.TimeoutError:
            logger.warning(
                "Timeout while fetching channel history",
                extra={"channel_id": channel_id, "thread_id": thread.id},
            )
            return False
        except Exception:
            logger.exception(
                "Failed to delete thread message", extra={"thread_id": thread.id}
            )
            return False

    async def initialize_chat_thread(self, ctx: interactions.SlashContext) -> bool:
        try:
            channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            if not isinstance(channel, interactions.GuildText):
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
            guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            if not guild:
                return False

            thread = await self.create_mediation_thread(channel, case_id)
            judges = await self.get_judges(guild)
            participants = frozenset({case.plaintiff_id, case.defendant_id} | judges)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(self.send_case_summary(thread, case_id, case))
                tg.create_task(self.update_case_with_thread(case_id, thread.id))
                tg.create_task(self.add_members_to_thread(thread, participants))

            return True
        except Exception as e:
            logger.exception(f"Mediation setup failed: {e}", extra={"case_id": case_id})
            return False

    async def send_case_summary(
        self,
        thread: interactions.ThreadChannel,
        case_id: str,
        case: Data,
    ) -> bool:
        try:
            components = self.view.create_action_buttons(case_id)
            embed = await self.view.create_summary_embed(case_id, case)
            await thread.send(
                embeds=[embed],
                components=components,
                allowed_mentions=interactions.AllowedMentions.none(),
            )
            return True
        except Exception:
            logger.exception("Failed to send case summary", extra={"case_id": case_id})
            return False

    async def update_case_with_thread(self, case_id: str, thread_id: int) -> bool:
        for attempt in range(3):
            try:
                async with asyncio.timeout(3.0):
                    await self.repo.update_case(
                        case_id,
                        thread_id=thread_id,
                        updated_at=datetime.now(timezone.utc).replace(microsecond=0),
                    )
                    return True
            except asyncio.TimeoutError:
                if attempt < 2:
                    await asyncio.sleep(2**attempt)
                    continue
            except Exception as e:
                logger.exception(
                    str(e), extra={"case_id": case_id, "thread_id": thread_id}
                )
                break
        return False

    @staticmethod
    def sanitize_input(text: str, max_length: int = 2000) -> str:
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()[:max_length]

    # Initialization

    @staticmethod
    async def create_mediation_thread(
        channel: interactions.GuildText,
        case_id: str,
    ) -> interactions.ThreadChannel:
        thread_name = f"Case #{case_id} Mediation Room"
        try:
            thread = await channel.create_thread(
                name=thread_name,
                thread_type=interactions.ChannelType.GUILD_PRIVATE_THREAD,
                reason=f"Mediation thread for case {case_id}",
            )
            logger.info("Created mediation thread", extra={"case_id": case_id})
            return thread
        except HTTPException:
            logger.error(
                "Failed to create mediation thread", extra={"case_id": case_id}
            )
            raise

    @staticmethod
    def create_case_id(length: int = 6) -> str:
        return secrets.token_bytes(length).hex()[:length]

    async def create_new_case(
        self,
        ctx: interactions.ModalContext,
        data: Dict[str, str],
        defendant_id: int,
    ) -> Tuple[Optional[str], Optional[Data]]:
        try:
            case_id = self.create_case_id()
            case = await self.create_case_data(ctx, data, defendant_id)
            await self.repo.persist_case(case_id, case)
            logger.info("Created new case", extra={"case_id": case_id})
            return case_id, case
        except Exception:
            logger.exception("Failed to create case")
            return None, None

    async def create_case_data(
        self,
        ctx: interactions.ModalContext,
        data: Dict[str, str],
        defendant_id: int,
    ) -> Data:
        sanitized = {k: self.sanitize_input(v) for k, v in data.items() if v}
        now = datetime.now(timezone.utc).replace(microsecond=0)
        return Data(
            plaintiff_id=ctx.author.id,
            defendant_id=defendant_id,
            accusation=sanitized.get("accusation", ""),
            facts=sanitized.get("facts", ""),
            status=CaseStatus.FILED,
            judges=frozenset(),
            allowed_roles=frozenset(),
            mute_list=frozenset(),
            roles={},
            evidence_queue=tuple(),
            thread_id=None,
            trial_thread_id=None,
            log_message_id=None,
            created_at=now,
            updated_at=now,
        )

    @staticmethod
    async def create_new_chat_thread(
        channel: interactions.GuildText,
    ) -> interactions.ThreadChannel:
        try:
            thread = await channel.create_thread(
                name="Chat Room",
                thread_type=interactions.ChannelType.GUILD_PUBLIC_THREAD,
                auto_archive_duration=interactions.AutoArchiveDuration.ONE_WEEK,
                reason="Chat room for lawsuit management",
            )
            logger.info("Created chat thread", extra={"thread_id": thread.id})
            return thread
        except HTTPException:
            logger.error("Failed to create chat thread")
            raise

    async def create_chat_room_setup_embed(
        self,
        thread: interactions.ThreadChannel,
    ) -> interactions.Embed:
        try:
            jump_url = f"https://discord.com/channels/{thread.guild.id}/{thread.id}"
            message = f"The lawsuit and appeal buttons have been sent to this channel. The chat room thread is ready: [Jump to Thread]({jump_url})."
            return await self.view.generate_embed(title="Success", description=message)
        except Exception:
            logger.error("Failed to create chat room setup embed")
            raise

    async def create_trial_thread(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        is_public: bool,
    ) -> None:
        try:
            thread_type = (
                interactions.ChannelType.GUILD_PUBLIC_THREAD
                if is_public
                else interactions.ChannelType.GUILD_PRIVATE_THREAD
            )
            thread_name = f"Case #{case_id} {('Public' if is_public else 'Private')} Trial Chamber"

            courtroom = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            allowed_roles = await self.get_allowed_members(ctx.guild)

            trial_thread = await courtroom.create_thread(
                name=thread_name,
                thread_type=thread_type,
                auto_archive_duration=interactions.AutoArchiveDuration.ONE_WEEK,
                reason=f"Trial thread for case {case_id}",
            )

            case.trial_thread_id = trial_thread.id
            case.allowed_roles = allowed_roles

            embed = await self.view.create_summary_embed(case_id, case)
            end_trial_button = self.view.create_end_trial_button(case_id)
            await trial_thread.send(embeds=[embed], components=[end_trial_button])

            if is_public:
                await self.delete_thread_created_message(
                    self.config.COURTROOM_CHANNEL_ID, trial_thread
                )

            visibility = "publicly" if is_public else "privately"
            await self.view.send_success(
                ctx, f"Case #{case_id} will be tried {visibility}."
            )
            await self.notify_participants(
                case_id, f"Case #{case_id} will be tried {visibility}."
            )
            await self.add_members_to_thread(trial_thread, case.judges)

            logger.info("Created trial thread", extra={"case_id": case_id})

        except HTTPException:
            logger.error("Failed to create trial thread", extra={"case_id": case_id})
            await self.view.send_error(
                ctx, f"Failed to create trial thread for case {case_id}."
            )
            raise
        except Exception:
            logger.exception("Unexpected error creating trial thread")
            await self.view.send_error(
                ctx, "An unexpected error occurred while setting up the trial thread."
            )
            raise

    # Retrieval

    @staticmethod
    async def get_allowed_members(guild: interactions.Guild) -> frozenset[int]:
        return frozenset(
            m.id
            for m in guild.members
            if any(r.name in {role.value for role in CaseRole} for r in m.roles)
        )

    async def get_judges(self, guild: interactions.Guild) -> frozenset[int]:
        if not (role := guild.get_role(self.config.JUDGE_ROLE_ID)):
            return frozenset()
        return frozenset(m.id for m in role.members)

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
        if threads := await channel.fetch_active_threads():
            thread_list = list(threads.threads)
            return next((t for t in thread_list if t.name == "Chat Room"), None)
        return None

    async def get_user_active_case_number(self, user_id: int) -> Optional[str]:
        cases = await self.repo.get_all_cases()
        for case_id, case_data in cases.items():
            case = Data.model_validate(case_data)
            if (
                user_id in {getattr(case, "thread_id"), getattr(case, "defendant_id")}
                and getattr(case, "status") != CaseStatus.CLOSED
            ):
                return case_id
        return None

    async def find_case_number_by_channel_id(self, channel_id: int) -> Optional[str]:
        cases = await self.repo.get_all_cases()
        for case_id, case_data in cases.items():
            case = Data.model_validate(case_data)
            if channel_id in {
                getattr(case, "plaintiff_id"),
                getattr(case, "trial_thread_id"),
            }:
                return case_id
        return None

    # Synchronization

    @staticmethod
    async def cancel_task(task: asyncio.Task) -> None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @contextlib.asynccontextmanager
    async def case_lock_manager(self, case_id: str) -> AsyncGenerator[None, None]:
        lock = self.repo.repo_case_locks[case_id]

        try:
            async with asyncio.timeout(3.0):
                async with lock:
                    try:
                        yield
                    except asyncio.CancelledError:
                        logger.info(
                            "Operation cancelled while holding lock",
                            extra={"case_id": case_id},
                        )
                        raise
        except asyncio.TimeoutError:
            logger.error("Lock acquisition timeout", extra={"case_id": case_id})
            raise
        except Exception as e:
            logger.exception(
                f"Lock error: {type(e).__name__}", extra={"case_id": case_id}
            )
            raise

    # Transcript

    async def enqueue_case_file_save(self, case_id: str, case: Data) -> None:
        async def save_case_file() -> None:
            try:
                log_channel = await self.bot.fetch_channel(self.config.LOG_CHANNEL_ID)
                log_forum = await self.bot.fetch_channel(self.config.LOG_FORUM_ID)
                log_post = await log_forum.fetch_message(self.config.LOG_POST_ID)

                file_name = f"case_{case_id}_{int(time.time())}.txt"
                content = await self.generate_case_transcript(case_id, case)

                async with aiofiles.tempfile.NamedTemporaryFile(
                    mode="w+", delete=False
                ) as temp_file:
                    await temp_file.write(content)
                    await temp_file.flush()
                    os.fsync(temp_file.fileno())

                    file = interactions.File(temp_file.name, file_name)
                    message = f"Case {case_id} details saved at {datetime.now(timezone.utc).isoformat()}"

                    await log_channel.send(message, files=[file])
                    await log_post.reply(message, files={"files": [file]})

                await aiofiles.os.remove(temp_file.name)

            except Exception:
                logger.exception(f"Failed to save case file {case_id}")
                raise

        await self.case_task_queue.put(save_case_file)

    async def generate_case_transcript(self, case_id: str, case: Data) -> str:
        header = await self.generate_report_header(
            case_id, case, datetime.now(timezone.utc)
        )

        thread_ids = [
            getattr(case, "thread_id", None),
            getattr(case, "trial_thread_id", None),
        ]
        transcripts = []

        for thread_id in thread_ids:
            if thread_id:
                channel = await self.bot.fetch_channel(thread_id)
                if transcript := await self.generate_thread_transcript(channel):
                    transcripts.append(transcript)

        return f"{header}{chr(10).join(transcripts)}"

    async def generate_thread_transcript(
        self, thread: Optional[interactions.GuildText]
    ) -> Optional[str]:
        if not thread:
            return None

        try:
            messages = await thread.history(limit=0).flatten()
            formatted = [
                await self.format_message_for_transcript(msg)
                for msg in reversed(messages)
            ]
            return f"\n## {thread.name}\n\n" + "\n".join(filter(None, formatted))
        except Exception as e:
            logger.exception(
                f"Thread transcript failed: {e}", extra={"thread_id": thread.id}
            )
            return None

    @staticmethod
    async def generate_report_header(case_id: str, case: Data, now: datetime) -> str:
        fields = {
            "Case ID": case_id,
            "Plaintiff ID": str(getattr(case, "plaintiff_id", None)),
            "Defendant ID": str(getattr(case, "defendant_id", None)),
            "Accusation": getattr(case, "accusation", "") or "",
            "Facts": getattr(case, "facts", "") or "",
            "Filing Time": now.isoformat(),
            "Case Status": getattr(getattr(case, "status", None), "name", "Unknown"),
        }
        return "".join(f"- **{k}:** {v}\n" for k, v in fields.items()) + "\n"

    @staticmethod
    async def format_message_for_transcript(msg: interactions.Message) -> str:
        lines = [f"{msg.author} at {msg.created_at.isoformat()}: {msg.content}\n"]
        if msg.attachments:
            lines.append(f"Attachments: {','.join(a.url for a in msg.attachments)}\n")
        if msg.edited_timestamp:
            lines.append(f"Edited at {msg.edited_timestamp.isoformat()}\n")
        return "".join(lines)
