from __future__ import annotations

import array
import asyncio
import collections
import contextlib
import dataclasses
import fcntl
import functools
import io
import itertools
import logging
import multiprocessing
import os
import random
import re
import secrets
import time
import types
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from enum import IntEnum, StrEnum, auto
from logging.handlers import RotatingFileHandler
from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
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
import sortedcontainers
import uvloop
from interactions.api.events import ExtensionUnload, MessageCreate, NewThreadCreate
from interactions.client.errors import Forbidden, HTTPException, NotFound

T = TypeVar("T", bound=object)
P = TypeVar("P", bound=object, contravariant=True)
R = TypeVar("R", bound=object)


ContextVarLock: TypeAlias = Dict[str, asyncio.Lock | None]


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


THREAD_POOL_SIZE: int = min(32, (os.cpu_count() or multiprocessing.cpu_count()) << 1)
thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(
    max_workers=THREAD_POOL_SIZE,
    thread_name_prefix="Lawsuit",
    initializer=lambda: os.nice(19),
)


aiofiles.open = functools.partial(
    aiofiles.open, mode="r+b", encoding=None, errors=None, closefd=True, opener=None
)


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
    GUILD_ID: int = dataclasses.field(
        default=1150630510696075404, metadata={"min": 0}, repr=False, compare=False
    )
    JUDGE_ROLE_ID: int = dataclasses.field(
        default=1200100104682614884, metadata={"min": 0}, repr=False, compare=False
    )
    PLAINTIFF_ROLE_ID: int = dataclasses.field(
        default=1200043628899356702, metadata={"min": 0}, repr=False, compare=False
    )
    COURTROOM_CHANNEL_ID: int = dataclasses.field(
        default=1247290032881008701, metadata={"min": 0}, repr=False, compare=False
    )
    LOG_CHANNEL_ID: int = dataclasses.field(
        default=1166627731916734504, metadata={"min": 0}, repr=False, compare=False
    )
    LOG_FORUM_ID: int = dataclasses.field(
        default=1159097493875871784, metadata={"min": 0}, repr=False, compare=False
    )
    LOG_POST_ID: int = dataclasses.field(
        default=1279118293936111707, metadata={"min": 0}, repr=False, compare=False
    )
    MAX_JUDGES_PER_LAWSUIT: int = dataclasses.field(
        default=1, metadata={"min": 1}, repr=False, compare=False
    )
    MAX_JUDGES_PER_APPEAL: int = dataclasses.field(
        default=3, metadata={"min": 1}, repr=False, compare=False
    )
    MAX_FILE_SIZE: int = dataclasses.field(
        default=10485760, metadata={"min": 0}, repr=False, compare=False
    )
    ALLOWED_MIME_TYPES: FrozenSet[str] = dataclasses.field(
        default_factory=lambda: frozenset(
            ("image/jpeg", "image/png", "application/pdf", "text/plain")
        ),
        metadata={"min_items": 1},
        repr=False,
        compare=False,
    )


class CaseStatus(StrEnum):
    FILED = auto()
    IN_PROGRESS = auto()
    CLOSED = auto()

    @classmethod
    def validate(cls, value: str) -> CaseStatus:
        try:
            return cls(value)
        except ValueError:
            raise ValueError(f"Invalid status: {value}. Must be one of {list(cls)}")


class EmbedColor(IntEnum):
    OFF = 0x5D5A58
    FATAL = 0xFF4343
    ERROR = 0xE81123
    WARN = 0xFFB900
    INFO = 0x0078D7
    DEBUG = 0x00B7C3
    TRACE = 0x8E8CD8
    ALL = 0x0063B1

    @classmethod
    def from_level(cls, level: str) -> EmbedColor:
        return getattr(cls, level.upper(), cls.INFO)


class CaseRole(StrEnum):
    PROSECUTOR = auto()
    PRIVATE_PROSECUTOR = auto()
    PLAINTIFF = auto()
    AGENT = auto()
    DEFENDANT = auto()
    DEFENDER = auto()
    WITNESS = auto()

    @classmethod
    def validate(cls, value: str) -> CaseRole:
        try:
            return cls(value)
        except ValueError:
            raise ValueError(f"Invalid role: {value}. Must be one of {list(cls)}")


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

    url: Annotated[
        str,
        pydantic.Field(
            description="Full yarl.URL to the attachment resource",
            frozen=True,
            repr=True,
        ),
    ]
    filename: Annotated[
        str,
        pydantic.Field(
            min_length=1,
            max_length=255,
            description="Name of the file with extension",
            frozen=True,
            pattern=r'^[^/\\:*?"<>|]+$',
        ),
    ]
    content_type: Annotated[
        str,
        pydantic.Field(
            min_length=1,
            max_length=255,
            pattern=r"^[\w-]+/[\w-]+$",
            description="MIME type of the attachment",
            frozen=True,
        ),
    ]


class Evidence(pydantic.BaseModel):
    user_id: Annotated[
        int,
        pydantic.Field(gt=0, description="ID of user submitting evidence", frozen=True),
    ]
    content: Annotated[
        str,
        pydantic.Field(
            min_length=1,
            max_length=4000,
            description="Main content of the evidence",
            frozen=True,
            pattern=r"^[\s\S]*$",
        ),
    ]
    attachments: Annotated[
        Tuple[Attachment, ...],
        pydantic.Field(
            default_factory=tuple,
            max_length=10,
            description="Tuple of evidence attachments",
            frozen=True,
        ),
    ]
    message_id: Annotated[
        int, pydantic.Field(gt=0, description="Discord message ID", frozen=True)
    ]
    timestamp: Annotated[
        datetime,
        pydantic.Field(
            default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0),
            description="UTC timestamp of evidence submission",
            frozen=True,
        ),
    ]
    state: Annotated[
        EvidenceAction,
        pydantic.Field(
            default=EvidenceAction.PENDING, description="Current state of the evidence"
        ),
    ]


class Data(pydantic.BaseModel):
    plaintiff_id: Annotated[int, pydantic.Field(gt=0, frozen=True)]
    defendant_id: Annotated[int, pydantic.Field(gt=0, frozen=True)]
    status: CaseStatus = pydantic.Field(default=CaseStatus.FILED)
    thread_id: Optional[Annotated[int, pydantic.Field(gt=0)]] = None
    judges: FrozenSet[int] = pydantic.Field(default_factory=frozenset)
    trial_thread_id: Optional[Annotated[int, pydantic.Field(gt=0)]] = None
    allowed_roles: FrozenSet[int] = pydantic.Field(default_factory=frozenset)
    log_message_id: Optional[Annotated[int, pydantic.Field(gt=0)]] = None
    mute_list: FrozenSet[int] = pydantic.Field(default_factory=frozenset)
    roles: Dict[str, FrozenSet[int]] = pydantic.Field(
        default_factory=lambda: types.MappingProxyType({}), frozen=True
    )
    evidence_queue: Tuple[Evidence, ...] = pydantic.Field(
        default_factory=tuple,
        max_length=100,
    )
    accusation: Optional[
        Annotated[
            str, pydantic.Field(max_length=1000, min_length=1, pattern=r"^[\s\S]*$")
        ]
    ] = None
    facts: Optional[
        Annotated[
            str, pydantic.Field(max_length=2000, min_length=1, pattern=r"^[\s\S]*$")
        ]
    ] = None
    created_at: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0),
        frozen=True,
    )
    updated_at: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0),
        frozen=True,
    )

    def lawsuit_model_dump(self, exclude_unset: bool = False) -> Dict[str, Any]:
        return {
            k: (
                list(v)
                if isinstance(v, frozenset)
                else (
                    {
                        sk: list(sv) if isinstance(sv, frozenset) else sv
                        for sk, sv in v.items()
                    }
                    if isinstance(v, dict)
                    else v
                )
            )
            for k, v in pydantic.BaseModel.model_dump(
                self, exclude_unset=exclude_unset
            ).items()
        }


class Store:
    def __init__(self) -> None:
        self.db_path = os.path.join(BASE_DIR, "cases.json")

    async def initialize_store(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            db_dir = os.path.dirname(self.db_path)
            await loop.run_in_executor(None, os.makedirs, db_dir, 0o755, True)
            await self.ensure_db_file()
        except OSError as e:
            logger.critical("Failed to initialize store: %s", repr(e))
            raise RuntimeError(
                f"Store initialization failed: {e.__class__.__name__}"
            ) from e

    async def ensure_db_file(self) -> None:
        try:
            if not await asyncio.to_thread(os.path.exists, self.db_path):
                async with aiofiles.open(self.db_path, mode="wb", buffering=0) as f:
                    await f.write(b"{}")
                    await f.flush()
                    await asyncio.to_thread(os.fsync, f.fileno())
                logger.info("Created JSON database: %s", self.db_path)
        except OSError:
            raise RuntimeError(
                f"Failed to create database file: {self.db_path}"
            ) from None

    @contextlib.asynccontextmanager
    async def file_lock(self, mode: str, shared: bool = False) -> AsyncIterator[Any]:
        loop = asyncio.get_running_loop()
        f = await aiofiles.open(self.db_path, mode=mode)
        try:
            lock_type = (
                fcntl.LOCK_SH | fcntl.LOCK_NB
                if shared
                else fcntl.LOCK_EX | fcntl.LOCK_NB
            )
            await loop.run_in_executor(None, fcntl.flock, f.fileno(), lock_type)
            try:
                yield f
            finally:
                await loop.run_in_executor(None, fcntl.flock, f.fileno(), fcntl.LOCK_UN)
        finally:
            await f.close()

    async def read_all(self) -> Dict[str, Dict[str, Any]]:
        async with self.file_lock("rb", shared=True) as f:
            return orjson.loads(memoryview(await f.read() or b"{}").tobytes())

    async def read_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        async with self.file_lock("rb", shared=True) as f:
            raw_data = await f.read()
            if not raw_data:
                return None
            return orjson.loads(memoryview(raw_data)).get(case_id, None)

    async def write_case(self, case_id: str, case_data: Dict[str, Any]) -> None:
        async with self.file_lock("r+b") as f:
            data = orjson.loads(memoryview((await f.read()) or b"{}"))
            data[case_id] = case_data
            serialized = orjson.dumps(
                data,
                option=orjson.OPT_INDENT_2
                | orjson.OPT_SORT_KEYS
                | orjson.OPT_SERIALIZE_NUMPY,
                default=lambda x: None,
            )
            await f.seek(0, os.SEEK_SET)
            await f.write(serialized)
            await f.truncate(len(serialized))
            await f.flush()
            os.fsync(f.fileno())

    async def delete_case(self, case_id: str) -> None:
        async with self.file_lock("r+b") as f:
            data = orjson.loads(memoryview(await f.read() or b"{}"))
            if case_id in data:
                data.pop(case_id, None)
                opts = (
                    orjson.OPT_INDENT_2
                    | orjson.OPT_SORT_KEYS
                    | orjson.OPT_SERIALIZE_NUMPY
                )
                serialized = orjson.dumps(data, option=opts)
                await f.seek(0)
                await f.write(serialized)
                await f.truncate(len(serialized))
                await f.flush()
                os.fsync(f.fileno())


class Repository(Generic[T]):
    def __init__(self) -> None:
        self.store = Store()
        self.cached_cases: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=1024, ttl=3600
        )
        self.repo_case_locks: collections.defaultdict[str, asyncio.Lock] = (
            collections.defaultdict(asyncio.Lock)
        )
        self.initialization_lock: asyncio.Lock = asyncio.Lock()
        self.initialized: bool = False

    async def initialize_repository(self) -> None:
        if self.initialized or self.initialization_lock.locked():
            return
        async with self.initialization_lock:
            if self.initialized:
                return
            await self.store.initialize_store()
            self.initialized = True
            logger.debug(
                "Repository initialized with optimized configuration",
                extra={"initialized": True},
            )

    @contextlib.asynccontextmanager
    async def case_lock_manager(self, case_id: str) -> AsyncIterator[None]:
        if not (isinstance(case_id, str)):
            raise TypeError(f"case_id must be str, got {type(case_id).__name__}")
        lock = self.repo_case_locks[case_id]
        try:
            async with asyncio.timeout(5.0), lock:
                yield None
        except asyncio.TimeoutError as e:
            logger.critical("Potential deadlock detected for case_id: %s", case_id)
            e.add_note(f"Lock acquisition timeout for case: {case_id}")
            raise

    async def get_case_data(self, case_id: str) -> Optional[T]:
        if not self.initialized:
            await self.initialize_repository()
        try:
            cached = self.cached_cases.get(case_id)
            if cached is not None:
                return cached

            case_data = await self.store.read_case(case_id)
            if case_data is None:
                return None

            instance = self.create_case_instance(
                dict(sorted(case_data.items(), key=lambda x: hash(x[0])))
            )
            if instance is None:
                return None

            self.cached_cases[case_id] = instance
            return instance
        except Exception as e:
            logger.exception(f"Critical error in get_case_data for {case_id}: {str(e)}")
            raise e from None

    async def persist_case(self, case_id: str, case: T) -> None:
        if not isinstance(case, Data):
            raise TypeError(f"Expected Data instance, got {type(case).__name__}")
        async with self.case_lock_manager(case_id):
            try:
                case_data = case.lawsuit_model_dump(exclude_unset=True)
                await asyncio.shield(self.store.write_case(case_id, case_data))
                self.cached_cases[case_id] = case
                logger.debug(f"Case {case_id} persisted successfully")
            except* Exception as e:
                logger.critical(
                    "Critical persistence failure",
                    extra={"case_id": case_id, "error": str(e)},
                    exc_info=True,
                )
                raise RuntimeError(f"Failed to persist case {case_id}") from e

    async def update_case(self, case_id: str, **kwargs: Any) -> None:
        async with self.case_lock_manager(case_id):
            if not (case := await asyncio.shield(self.get_case_data(case_id))):
                raise KeyError(f"Case {case_id} not found")
            try:
                updated_case = await asyncio.to_thread(
                    lambda: self.update_case_instance(
                        case, **{k: v for k, v in kwargs.items() if v is not None}
                    )
                )
                await asyncio.shield(self.persist_case(case_id, updated_case))
            except pydantic.ValidationError as e:
                logger.error(
                    f"Validation failed for case {case_id} update: {e}",
                    extra={"case_id": case_id, "error": str(e)},
                )
                raise RuntimeError(f"Validation error updating case {case_id}") from e

    async def delete_case(self, case_id: str) -> None:
        async with self.case_lock_manager(case_id):
            try:
                await asyncio.shield(self.store.delete_case(case_id))
                self.cached_cases.pop(case_id, None)
                self.repo_case_locks.pop(case_id, None)
            except* Exception as e:
                logger.critical(
                    "Case deletion failed",
                    extra={"case_id": case_id, "error": str(e)},
                    exc_info=True,
                )
                raise RuntimeError(f"Failed to delete case {case_id}") from e

    async def get_all_cases(self) -> Dict[str, T]:
        if not self.initialized:
            await self.initialize_repository()
        case_data = await self.store.read_all()
        return dict(
            itertools.compress(
                (
                    (
                        case_id,
                        self.cached_cases.setdefault(
                            case_id,
                            self.create_case_instance(
                                dict(sorted(data.items(), key=lambda x: x[0]))
                            ),
                        ),
                    )
                    for case_id, data in case_data.items()
                ),
                (
                    self.validate_case_data(case_id, data)
                    for case_id, data in case_data.items()
                ),
            )
        )

    def validate_case_data(self, case_id: str, data: Dict[str, Any]) -> bool:
        try:
            sorted_data = dict(sorted(data.items(), key=lambda x: x[0]))
            self.__class__.create_case_instance(sorted_data)
            return True
        except pydantic.ValidationError as e:
            logger.error(
                "Validation error for case %(case_id)s: %(errors)s",
                {"case_id": case_id, "errors": e.errors()},
            )
            return False

    @staticmethod
    def create_case_instance(data: Mapping[str, Any]) -> T:
        return Data(**({} | data if isinstance(data, dict) else dict(data)))

    @staticmethod
    def update_case_instance(case: T, **kwargs: Any) -> T:
        return Data(**dict(case.__dict__.items() | kwargs.items()))


# View


class View:

    PROVERBS: Tuple[str, ...] = (
        "Iuris prudentia est divinarum atque humanarum rerum notitia, iusti atque iniusti scientia (D. 1, 1, 10, 2).",
        "Iuri operam daturum prius nosse oportet, unde nomen iuris descendat. est autem a iustitia appellatum: nam, ut eleganter celsus definit, ius est ars boni et aequi (D. 1, 1, 1, pr.).",
        "Iustitia est constans et perpetua voluntas ius suum cuique tribuendi (D. 1, 1, 10, pr.).",
    )

    def __init__(self, bot: interactions.Client) -> None:
        self.bot: interactions.Client = bot
        self.config: Config = Config()
        self.embed_cache: Dict[str, interactions.Embed] = {}
        self.button_cache: Dict[str, List[interactions.Button]] = (
            collections.defaultdict(list)
        )
        self.proverb_cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=max(366, len(self.PROVERBS)), ttl=86400
        )
        self.cached_cases: types.MappingProxyType[str, Data] = types.MappingProxyType(
            dict()
        )
        self.proverb_selector: Callable[[date], str] = functools.lru_cache(maxsize=366)(
            self.create_proverb_selector()
        )
        self.embed_lock: asyncio.Lock = asyncio.Lock()
        self.button_lock: asyncio.Lock = asyncio.Lock()
        logger.debug(
            "View initialized with optimized caching and thread-safe primitives"
        )

    async def generate_embed(
        self,
        title: str,
        description: str = "",
        color: EmbedColor = EmbedColor.INFO,
        retry_attempts: int = 3,
    ) -> interactions.Embed:
        cache_key = f"{title}:{description}:{color.value}"

        if embed := self.embed_cache.get(cache_key):
            return embed

        async with self.embed_lock:
            if embed := self.embed_cache.get(cache_key):
                return embed

            for attempt in range(retry_attempts):
                try:
                    guild_task = self.bot.fetch_guild(self.config.GUILD_ID)
                    color_value = int(
                        color.value if isinstance(color, EmbedColor) else color
                    )

                    embed = interactions.Embed(
                        title=title,
                        description=description,
                        color=color_value,
                    )

                    guild = await guild_task

                    if guild_icon := getattr(guild, "icon", None):
                        footer_text = guild.name
                        footer_icon = str(guild_icon.url)
                    else:
                        footer_text = "鍵政大舞台"
                        footer_icon = None

                    embed.timestamp = datetime.now(timezone.utc)
                    embed.set_footer(text=footer_text, icon_url=footer_icon)
                    self.embed_cache[cache_key] = embed
                    return embed

                except HTTPException as e:
                    if attempt >= retry_attempts - 1:
                        logger.error(
                            "Failed to create embed after %(attempts)s attempts: %(error)s",
                            {"attempts": retry_attempts, "error": str(e)},
                            exc_info=True,
                        )
                        raise
                    await asyncio.sleep(1 << attempt)

                except Exception as e:
                    logger.error(
                        "Critical error in embed generation: %(error)s",
                        {"error": str(e)},
                        exc_info=True,
                    )
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

        def backoff(x: int) -> int:
            return min(32, 2**x)

        for attempt in range(retry_attempts):
            try:
                async with asyncio.timeout(5.0):
                    await ctx.send(embed=embed, ephemeral=True)
                    logger.debug(f"Response delivered on attempt {attempt + 1}")
                    return

            except asyncio.TimeoutError:
                continue

            except HTTPException as exc:
                if exc.status == 404 and attempt < retry_attempts - 1:
                    try:
                        async with asyncio.timeout(5.0):
                            await ctx.send(embed=embed, ephemeral=True)
                            logger.debug("Followup sent for expired interaction")
                            return
                    except (HTTPException, ConnectionError) as e:
                        if attempt == retry_attempts - 1:
                            logger.error(f"Failed followup: {e}", exc_info=True)
                            raise
                else:
                    logger.error(f"HTTP error in delivery: {exc}", exc_info=True)
                    raise

            except (ConnectionError, ValueError) as e:
                if attempt == retry_attempts - 1:
                    logger.error(f"Error in delivery: {e}", exc_info=True)
                    raise

            await asyncio.sleep(backoff(attempt))

    async def send_error(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.deliver_response(ctx, "Error", message, EmbedColor.ERROR)

    async def send_success(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.deliver_response(ctx, "Success", message, EmbedColor.INFO)

    @functools.lru_cache()
    def lawsuit_modal_store(self) -> cachetools.TTLCache:
        return cachetools.TTLCache(maxsize=2, ttl=3600)

    def create_lawsuit_modal(self, is_appeal: bool = False) -> interactions.Modal:
        cache_key = is_appeal
        cache = self.lawsuit_modal_store()

        if modal := cache.get(cache_key):
            return modal

        fields = self.define_modal_fields(is_appeal)
        numbered_fields = tuple(
            type(field)(
                custom_id=f"{field.custom_id}_{i}",
                label=field.label,
                placeholder=field.placeholder,
                required=field.required,
                min_length=field.min_length,
                max_length=field.max_length,
            )
            for i, field in enumerate(fields)
        )

        form_type = "appeal" if is_appeal else "lawsuit"
        modal = interactions.Modal(
            *numbered_fields,
            title=f"{form_type.title()} Form",
            custom_id=f"{form_type}_form_modal",
        )

        cache[cache_key] = modal
        return modal

    @staticmethod
    def define_modal_fields(
        is_appeal: bool,
    ) -> tuple[Union[interactions.ShortText, interactions.ParagraphText], ...]:
        base_configs = {
            True: (
                ("case_number", "Case Number", "Enter the original case number"),
                ("appeal_reason", "Reason for Appeal", "Enter the reason for appeal"),
            ),
            False: (
                ("defendant_id", "Defendant ID", "Enter the defendant's user ID"),
                ("accusation", "Accusation", "Enter the accusation"),
            ),
        }

        return tuple(
            interactions.ShortText(
                custom_id=custom_id,
                label=label,
                placeholder=placeholder,
                min_length=1,
                max_length=4000,
            )
            for custom_id, label, placeholder in base_configs[is_appeal]
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
    @functools.lru_cache(maxsize=1)
    def action_buttons_store(self) -> interactions.ActionRow:
        return interactions.ActionRow(
            *(
                interactions.Button(
                    style=getattr(interactions.ButtonStyle, style),
                    label=label,
                    custom_id=f"initiate_{action}_button",
                )
                for style, label, action in (
                    ("PRIMARY", "File Lawsuit", "lawsuit"),
                    ("SECONDARY", "File Appeal", "appeal"),
                )
                if style and label and action
            )
        )

    async def create_summary_embed(
        self, case_id: str, case: Data
    ) -> interactions.Embed:
        embed = await self.generate_embed(f"Case #{case_id}")
        field_names = (
            "Presiding Judge",
            "Plaintiff",
            "Defendant",
            "Accusation",
            "Facts",
        )

        def field_values(name: str) -> str:
            return (
                " ".join(f"<@{judge}>" for judge in case.judges) or "Not yet assigned"
                if name == "Presiding Judge"
                else (
                    f"<@{getattr(case, f'{name.lower()}_id')}>"
                    if name in frozenset(("Plaintiff", "Defendant"))
                    else getattr(case, name.lower()) or "None"
                )
            )

        embed.add_fields(
            *(
                interactions.EmbedField(
                    name=name,
                    value=field_values(name),
                    inline=False,
                )
                for name in field_names
            )
        )
        logger.debug("Created optimized summary embed: case_id=%s", case_id)
        return embed

    @staticmethod
    def create_action_buttons(case_id: str) -> List[interactions.ActionRow]:
        return [
            interactions.ActionRow(
                *(
                    interactions.Button(
                        style=getattr(interactions.ButtonStyle, style),
                        label=label,
                        custom_id=f"{action}_{case_id}",
                    )
                    for style, label, action in (
                        ("DANGER", "Dismiss", "dismiss"),
                        ("SUCCESS", "Accept", "accept"),
                        ("SECONDARY", "Withdraw", "withdraw"),
                    )
                )
            )
        ]

    async def create_trial_privacy_buttons(
        self, case_id: str
    ) -> Tuple[interactions.Button, ...]:
        cache_key: str = f"trial_privacy_{case_id}"

        if buttons := self.button_cache.get(cache_key):
            return tuple(buttons)

        async with self.button_lock:
            if buttons := self.button_cache.get(cache_key):
                return tuple(buttons)

            buttons = [
                interactions.Button(
                    style=getattr(interactions.ButtonStyle, style),
                    label=label,
                    custom_id=f"public_trial_{action}_{case_id}",
                )
                for style, label, action in (
                    ("SUCCESS", "Yes", "yes"),
                    ("DANGER", "No", "no"),
                )
            ]

            self.button_cache[cache_key] = list(buttons)
            logger.debug("Created and cached privacy buttons: case_id=%s", case_id)
            return tuple(buttons)

    @staticmethod
    def create_end_trial_button(case_id: str) -> interactions.Button:
        button = interactions.Button(
            style=interactions.ButtonStyle.DANGER,
            label="End Trial",
            custom_id=f"end_trial_{case_id}",
        )
        logger.debug(f"Created optimized end trial button: case_id={case_id}")
        return button

    @staticmethod
    def create_user_management_buttons(user_id: int) -> Tuple[interactions.Button, ...]:
        return tuple(
            interactions.Button(
                style=style, label=label, custom_id=f"{action}_{user_id}"
            )
            for label, action, style in (
                ("Mute", "mute", interactions.ButtonStyle.PRIMARY),
                ("Unmute", "unmute", interactions.ButtonStyle.SUCCESS),
            )
        )

    @staticmethod
    def create_message_management_buttons(
        message_id: int,
    ) -> Tuple[interactions.Button, ...]:
        return tuple(
            interactions.Button(
                style=getattr(interactions.ButtonStyle, style),
                label=label,
                custom_id=f"{action}_{message_id}",
            )
            for style, label, action in (
                ("PRIMARY", "Pin", "pin"),
                ("DANGER", "Delete", "delete"),
            )
        )

    @staticmethod
    def create_evidence_action_buttons(
        case_id: str, user_id: int
    ) -> Tuple[interactions.Button, ...]:
        return tuple(
            interactions.Button(
                style=getattr(interactions.ButtonStyle, style),
                label=label,
                custom_id=f"{action}_evidence_{case_id}_{user_id}",
            )
            for style, label, action in (
                ("SUCCESS", "Make Public", "approve"),
                ("DANGER", "Keep Private", "reject"),
            )
        )

    def create_proverb_selector(self) -> Callable[[date], str]:
        proverb_count: int = len(self.PROVERBS)
        shuffled_indices = array.array(
            "L", random.SystemRandom().sample(range(proverb_count), proverb_count)
        )
        mask = proverb_count - 1
        logger.debug("Created optimized proverb selector with secure randomization")

        def select_proverb(current_date: date, _mask: int = mask) -> str:
            index_position = (
                current_date.toordinal() & _mask
                if bin(_mask + 1).count("1") == 1
                else current_date.toordinal() % _mask
            )
            proverb = self.PROVERBS[shuffled_indices[index_position]]
            logger.debug(f"Selected optimized proverb for {current_date}")
            return proverb

        return select_proverb

    def select_daily_proverb(self, current_date: Optional[date] = None) -> str:
        cache_key = (current_date or date.today()).toordinal()
        try:
            return self.proverb_cache[cache_key]
        except KeyError:
            proverb = self.proverb_selector(date.fromordinal(cache_key))
            self.proverb_cache[cache_key] = proverb
            logger.debug(
                "Selected and cached proverb for %s",
                date.fromordinal(cache_key).isoformat(),
                extra={"cache_key": cache_key, "proverb_length": len(proverb)},
            )
            return proverb

    def clear_caches(self) -> None:
        try:
            for lock in (self.embed_lock, self.button_lock):
                if lock.locked():
                    lock.release()

            self.embed_cache.clear()
            self.button_cache.clear()

            for cache in (
                self.lawsuit_modal_store(),
                self.proverb_cache,
            ):
                cache.clear()
                del cache

            logger.info("Cache clearing completed with optimal thread safety")

        except Exception as e:
            logger.critical(
                "Critical error during cache clearing: %s", e, exc_info=True
            )
            raise RuntimeError("Cache clearing failed", e.__class__.__name__) from e


async def user_is_judge(ctx: interactions.BaseContext) -> bool:
    judge_id: int = Config().JUDGE_ROLE_ID
    result: bool = any(r.id == judge_id for r in ctx.author.roles)
    logger.debug(
        "Judge role validation: %s",
        result,
        extra={"user_id": ctx.author.id, "judge_id": judge_id},
    )
    return result


# Controller


class Lawsuit(interactions.Extension):
    def __init__(self, bot: interactions.Client) -> None:
        self.bot = bot
        self.config = Config()
        self.store = Store()
        self.repo: Repository = Repository()
        self.view = View(bot)
        self.cached_cases: dict[str, Repository] = {}
        self.case_data_cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=1000, ttl=300
        )
        self.lock_timestamps = sortedcontainers.SortedDict()
        self.case_lock = asyncio.Lock()
        self.case_task_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10000)
        self.shutdown_event = asyncio.Event()
        self.lawsuit_button_message_id = None
        self.lock_contention: dict[str, int] = {}
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
        self.case_locks = cachetools.TTLCache(maxsize=1000, ttl=300)
        self.init_task = asyncio.create_task(
            self.initialize_background_tasks(), name="LawsuitInitialization"
        )
        self.init_task.add_done_callback(lambda t: self.handle_init_exception(t))
        self.cleanup_task = None
        self._case_locks = sortedcontainers.SortedDict()

    # Tasks

    async def initialize_background_tasks(self) -> None:
        tasks = {
            "_fetch_cases": self.fetch_cases,
            "_daily_embed_update": self.daily_embed_update,
            "_process_task_queue": self.process_task_queue,
        }
        for name, coro in tasks.items():
            task = asyncio.create_task(coro(), name=name)
            task.add_done_callback(self.handle_task_exception)

    @staticmethod
    def handle_init_exception(future: asyncio.Future[None]) -> None:
        try:
            future.result()
        except Exception as e:
            logger.critical(
                "Critical initialization failure",
                exc_info=True,
                extra={"error": str(e)},
            )

    def handle_task_exception(self, task: asyncio.Task[Any]) -> None:
        task_name = task.get_name()
        if task.cancelled():
            logger.info(f"Task {task_name} cancelled gracefully")
            return

        if exc := task.exception():
            if isinstance(exc, asyncio.CancelledError):
                logger.info(f"Task {task_name} cancelled via CancelledError")
            else:
                logger.critical(
                    f"Critical task failure in {task_name}",
                    exc_info=exc,
                    extra={"task_name": task_name},
                )
                self.restart_task(task)

    def restart_task(self, task: asyncio.Task[Any]) -> None:
        task_name = task.get_name().lower()
        task_method = getattr(self, f"_{task_name}", None)
        if task_method is None:
            logger.error(
                "Task restart failed - method not found: %(name)s",
                {"name": task_name},
                extra={"task_name": task_name},
            )
            return

        new_task = asyncio.create_task(task_method(), name=task.get_name())
        new_task.add_done_callback(lambda t: self.handle_task_exception(t))
        logger.info(
            "Task %(name)s restarted successfully",
            {"name": task.get_name()},
        )

    async def cleanup_locks(self) -> None:
        try:
            async with contextlib.AsyncExitStack() as stack:
                await stack.enter_async_context(self.case_lock)
                self._case_locks.clear()
                self.lock_timestamps.clear()
                self.lock_contention.clear()
                logger.info(
                    "Lock cleanup completed",
                    extra={
                        "locks_cleared": len(self._case_locks),
                        "timestamps_cleared": len(self.lock_timestamps),
                        "contention_cleared": len(self.lock_contention),
                    },
                )
        except Exception as e:
            logger.exception(
                "Lock cleanup failed",
                extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    def __del__(self) -> None:
        try:
            cleanup_task = object.__getattribute__(self, "cleanup_task")
            if cleanup_task is not None and isinstance(cleanup_task, asyncio.Task):
                if not cleanup_task._state:
                    cleanup_task.cancel()
                    logger.info(
                        "Cleanup task cancelled during deletion",
                        extra={"task_id": id(cleanup_task)},
                    )
        except AttributeError:
            pass

    async def fetch_cases(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                async with asyncio.timeout(30):
                    cases = await self.repo.get_all_cases()
                    self.cached_cases.clear()
                    self.cached_cases.update(cases)
                    logger.info(
                        "Cases fetched successfully",
                        extra={"case_count": len(self.cached_cases)},
                    )
                    await asyncio.sleep(min(300, max(60, len(self.cached_cases) // 2)))
            except (asyncio.TimeoutError, Exception) as e:
                self.cached_cases.clear()
                logger.exception(
                    "Case fetch failed",
                    extra={
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "retry_delay": 60,
                    },
                )
                await asyncio.sleep(60)

    async def process_task_queue(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                task_callable = await asyncio.wait_for(
                    self.case_task_queue.get_nowait(), timeout=1.0
                )
            except asyncio.QueueEmpty:
                continue
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
            except* (Exception, asyncio.CancelledError) as eg:
                logger.exception(
                    "Task processing failed",
                    extra={
                        "error": str(eg.exceptions[0]),
                        "task_id": id(task_callable),
                    },
                )
            finally:
                self.case_task_queue.task_done()
                logger.debug("Task completed", extra={"task_id": id(task_callable)})

    async def daily_embed_update(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                async with asyncio.timeout(86400):
                    await asyncio.sleep(86400)
                    await asyncio.shield(self.update_lawsuit_button_embed())
            except asyncio.CancelledError:
                logger.info(
                    "Embed update cancelled", extra={"timestamp": time.time_ns()}
                )
                return
            except asyncio.TimeoutError:
                continue
            except Exception as eg:
                logger.error(
                    "Embed update failed",
                    exc_info=True,
                    extra={
                        "error": str(eg),
                        "error_type": type(eg).__name__,
                        "timestamp": time.time_ns(),
                    },
                )

    async def update_lawsuit_button_embed(self) -> None:
        if not (button_id := self.lawsuit_button_message_id):
            logger.warning("Button message ID not set", extra={"button_id": None})
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
                logger.info("Embed updated", extra={"message_id": button_id})

        except asyncio.TimeoutError:
            logger.error("Embed update timed out", extra={"button_id": button_id})
        except NotFound:
            logger.error("Button message not found", extra={"button_id": button_id})
        except HTTPException as e:
            logger.exception(
                "HTTP error during embed update",
                extra={"error": str(e), "code": getattr(e, "code", None)},
            )
        except Exception as e:
            logger.exception(
                "Critical embed update failure",
                extra={
                    "error": str(e),
                    "type": type(e).__name__,
                    "button_id": button_id,
                },
            )

    # Listeners

    @interactions.listen(NewThreadCreate)
    async def on_thread_create(self, event: NewThreadCreate) -> None:
        if event.thread.parent_id != self.config.COURTROOM_CHANNEL_ID:
            return

        try:
            async with asyncio.timeout(3.0):
                await self.delete_thread_created_message(
                    event.thread.parent_id, event.thread
                )
                logger.debug(
                    "Thread deletion completed",
                    extra={
                        "thread_id": event.thread.id,
                        "parent_id": event.thread.parent_id,
                        "timestamp": time.time_ns(),
                    },
                )
        except asyncio.TimeoutError:
            logger.error(
                "Thread deletion timed out", extra={"thread_id": event.thread.id}
            )
            raise
        except Exception as e:
            logger.exception(
                "Thread deletion failed",
                extra={
                    "thread_id": event.thread.id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise

    @interactions.listen(MessageCreate)
    async def on_message_create(self, event: MessageCreate) -> None:
        msg = event.message
        if msg.author.id == self.bot.user.id:
            return

        try:
            is_dm = isinstance(msg.channel, interactions.DMChannel)
            handler = (
                self.handle_direct_message_evidence
                if is_dm
                else self.handle_channel_message
            )
            await handler(msg)
            logger.debug(
                f"{handler.__name__.partition('handle_')[2].title()} processed",
                extra={
                    (is_dm and "user_id" or "channel_id"): (
                        is_dm and msg.author.id or msg.channel.id
                    )
                },
            )
        except Exception as e:
            logger.exception(
                "Message processing failed",
                extra={"message_id": msg.id, "error": str(e)},
            )
            raise

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self) -> None:
        logger.info("Initiating Lawsuit extension unload sequence")
        self.shutdown_event.set()

        if _cleanup_task := self.cleanup_task:
            if not _cleanup_task.done():
                try:
                    _cleanup_task.cancel()
                    async with asyncio.timeout(5.0):
                        try:
                            await _cleanup_task
                        except asyncio.CancelledError:
                            pass
                    logger.debug("Cleanup task cancelled successfully")
                except asyncio.TimeoutError:
                    logger.error("Cleanup task cancellation timed out")
                except Exception as eg:
                    logger.exception(
                        "Cleanup task cancellation failed",
                        extra={"error": str(eg), "error_type": type(eg).__name__},
                    )
                    raise eg from None

        try:
            async with asyncio.timeout(10.0):
                await self.cleanup_locks()
        except* Exception as eg:
            exc = eg.exceptions[0]
            logger.exception(
                "Lock cleanup failed during unload",
                extra={"error": str(exc), "error_type": type(exc).__name__},
            )
            raise exc from None

        logger.info("Lawsuit extension unloaded successfully")

    # Component

    @interactions.component_callback("initiate_lawsuit_button")
    async def initiate_lawsuit(self, ctx: interactions.ComponentContext) -> None:
        try:
            author_id = ctx.author.id
            if await self.has_ongoing_lawsuit(author_id):
                logger.debug(f"Duplicate lawsuit attempt by user {author_id}")
                return await self.view.send_error(
                    ctx, "You already have an ongoing lawsuit"
                )

            modal = self.view.create_lawsuit_modal()
            await ctx.send_modal(modal)
            logger.info("Lawsuit modal sent", extra={"user_id": author_id})

        except Exception as e:
            error_data = {"user_id": ctx.author.id, "error": str(e)}
            logger.exception("Lawsuit initiation failed", extra=error_data)
            await self.view.send_error(
                ctx, "An unexpected error occurred. Please try again later."
            )
            raise

    @interactions.modal_callback("lawsuit_form_modal")
    async def submit_lawsuit_form(self, ctx: interactions.ModalContext) -> None:
        try:
            await ctx.defer(ephemeral=True)
            logger.debug(f"Processing lawsuit form: {ctx.responses}")

            responses = {k: v.strip() for k, v in ctx.responses.items()}
            if not responses or not all(responses.values()):
                await self.view.send_error(
                    ctx, "Please ensure all fields are properly filled"
                )
                return

            defendant_id_str = responses.get("defendant_id_0", "")
            try:
                defendant_id = int("".join(c for c in defendant_id_str if c.isdigit()))
                if defendant_id == ctx.author.id:
                    await self.view.send_error(ctx, "Self-litigation is not permitted")
                    return

                user = await self.bot.fetch_user(defendant_id)
                if not user:
                    await self.view.send_error(ctx, "Invalid defendant ID provided")
                    return

            except (ValueError, NotFound):
                await self.view.send_error(
                    ctx, "Invalid defendant ID or user not found"
                )
                return

            sanitized_data = dict(
                defendant_id=str(defendant_id),
                accusation=responses.get("accusation_1", ""),
                facts=responses.get("facts_2", ""),
            )

            try:
                case_id, case = await self.create_new_case(
                    ctx, sanitized_data, defendant_id
                )
                if not (case_id and case):
                    await self.view.send_error(ctx, "Failed to create case")
                    return

                await self.setup_mediation_thread(case_id, case)
                await self.notify_judges(case_id, ctx.guild_id)
                await self.view.send_success(
                    ctx, f"Mediation room #{case_id} created successfully"
                )

            except Exception:
                logger.exception("Failed to setup case")
                await self.view.send_error(ctx, "Failed to setup the case")
                return

        except Exception as e:
            logger.exception("Lawsuit submission failed", extra={"error": str(e)})
            await self.view.send_error(
                ctx, "An unexpected error occurred. Please try again later."
            )

    @interactions.component_callback("dismiss_*", "accept_*", "withdraw_*")
    async def handle_case_actions(self, ctx: interactions.ComponentContext) -> None:
        action, case_id = ctx.custom_id.split("_", 1)
        await self.process_case_action(ctx, CaseAction(action.upper()), case_id)

    @interactions.component_callback("public_trial_*")
    async def handle_trial_privacy(self, ctx: interactions.ComponentContext) -> None:
        try:
            _, _, remainder = ctx.custom_id.partition("public_trial_")
            is_public_str, _, case_id = remainder.partition("_")

            async with (
                asyncio.timeout(5.0),
                self.repo.case_lock_manager(case_id) as case,
            ):
                if not case:
                    await self.view.send_error(ctx, "Case not found")
                    logger.warning("Case %s not found", case_id)
                    return

                is_public = is_public_str.lower() == "yes"

                async with asyncio.timeout(10.0):
                    await self.create_trial_thread(
                        ctx=ctx, case_id=case_id, case=case, is_public=is_public
                    )

                logger.info(
                    "Trial privacy set",
                    extra={"case_id": case_id, "is_public": is_public},
                )

        except (asyncio.TimeoutError, ValueError) as e:
            logger.error(
                "Trial privacy error",
                exc_info=True,
                extra={"error_type": type(e).__name__, "custom_id": ctx.custom_id},
            )
            await self.view.send_error(ctx, "Request timed out or had invalid format")
            raise
        except Exception as e:
            logger.exception(
                "Trial privacy critical failure",
                extra={"error": str(e), "error_type": type(e).__name__},
            )
            await self.view.send_error(ctx, "Failed to set trial privacy")
            raise

    @interactions.component_callback("mute_*", "unmute_*")
    async def handle_user_mute(self, ctx: interactions.ComponentContext) -> None:
        try:
            action_str, user_id_str = next(iter([ctx.custom_id.split("_", 1)]))
            case_id = await self.validate_user_permissions(ctx.author.id, user_id_str)

            if not case_id:
                await self.view.send_error(ctx, "Associated case not found")
                logger.warning("Case not found for channel")
                return

            await self.process_mute_action(
                ctx=ctx,
                case_id=str(case_id),
                target_id=int(user_id_str, base=10),
                mute=action_str.__eq__("mute"),
            )

        except ValueError as ve:
            logger.error("Invalid user ID: %s", user_id_str)
            await self.view.send_error(ctx, "Invalid user ID format")
            raise ve from None

    @interactions.component_callback("pin_*", "delete_*")
    async def handle_message_action(self, ctx: interactions.ComponentContext) -> None:
        try:
            action_str, message_id_str = ctx.custom_id.partition("_")[::2]
            case_id = await self.validate_user_permissions(
                ctx.author.id, message_id_str
            )

            if case_id is None:
                logger.warning("Case not found for channel")
                await self.view.send_error(ctx, "Associated case not found")
                return

            await self.process_message_action(
                ctx=ctx,
                case_id=str(case_id),
                target_id=int(message_id_str),
                action_type=MessageAction(action_str),
            )

        except ValueError as ve:
            logger.error("Invalid message ID: %s", message_id_str)
            await self.view.send_error(ctx, "Invalid message ID format")
            raise ve from None

    @interactions.component_callback("approve_evidence_*", "reject_evidence_*")
    async def handle_evidence_action(self, ctx: interactions.ComponentContext) -> None:
        try:
            action_str, _, remainder = ctx.custom_id.partition("_evidence_")
            case_id, _, user_id_str = remainder.partition("_")

            action_map = {
                "approve": EvidenceAction.APPROVED,
                "reject": EvidenceAction.REJECTED,
            }

            await self.process_judge_evidence_decision(
                ctx=ctx,
                action=action_map[action_str],
                case_id=case_id,
                user_id=int.__new__(int, user_id_str),
            )
        except (ValueError, KeyError) as e:
            logger.error("Invalid format: %s", ctx.custom_id)
            await self.view.send_error(ctx, "Invalid request format")
            raise e from None

    @interactions.component_callback("end_trial_*")
    async def handle_end_trial(self, ctx: interactions.ComponentContext) -> None:
        await self.process_case_action(ctx, CaseAction.CLOSE, ctx.custom_id[10:])

    # Command handlers

    module_base: interactions.SlashCommand = interactions.SlashCommand(
        name="lawsuit",
        description="Lawsuit management system",
    )

    @module_base.subcommand(
        "dispatch",
        sub_cmd_description="Deploy the lawsuit management interface",
    )
    @interactions.check(user_is_judge)
    async def deploy_lawsuit_button(self, ctx: interactions.SlashContext) -> None:
        await ctx.defer(ephemeral=True)
        try:
            async with asyncio.timeout(5.0), contextlib.AsyncExitStack():
                action_row = self.view.action_buttons_store
                daily_proverb = self.view.select_daily_proverb()

                embed = await self.view.generate_embed(
                    title=daily_proverb,
                    description="Click below to file a lawsuit or appeal",
                )

                message = await ctx.channel.send(
                    embeds=[embed],
                    components=[action_row],
                    allowed_mentions=interactions.AllowedMentions.none(),
                )

                self.lawsuit_button_message_id = message.id
                await self.initialize_chat_thread(ctx)

                logger.info(
                    "Lawsuit interface deployed",
                    extra=dict(
                        zip(
                            ("user_id", "channel_id", "message_id"),
                            (ctx.author.id, ctx.channel_id, message.id),
                        )
                    ),
                )

        except* Exception as eg:
            exc = eg.__cause__
            match exc:
                case HTTPException() if isinstance(
                    exc, HTTPException
                ) and exc.status == 404:
                    logger.warning("Interaction expired")
                case HTTPException():
                    logger.error(
                        "HTTP error in lawsuit deployment: %d",
                        exc.status,
                        exc_info=True,
                    )
                case _:
                    logger.exception("Lawsuit interface deployment failed")
            await self.view.send_error(ctx, "Failed to deploy interface")
            raise exc from None

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
            interactions.SlashCommandChoice(name=a.name.title(), value=a.value)
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
            async with asyncio.timeout(5.0):
                case_id = self.find_case_number_by_channel_id(ctx.channel_id)
                if not case_id:
                    raise ValueError("No associated case found")

                interaction_action = RoleAction(action.lower())
                await self.manage_case_role(
                    ctx,
                    case_id,
                    interaction_action,
                    role,
                    user,
                )

                action_str = (
                    "assigned to"
                    if interaction_action == RoleAction.ASSIGN
                    else "revoked from"
                )

                logger.info(
                    "Role management completed",
                    extra={
                        "action": action_str,
                        "role": role,
                        "user_id": user.id,
                        "case_id": case_id,
                        "moderator_id": ctx.author.id,
                    },
                )

        except* Exception as eg:
            error_mapping: dict[type[Exception], str] = {
                ValueError: "Invalid configuration",
                asyncio.TimeoutError: "Operation timed out",
            }
            error_type = type(eg.__cause__ if eg.__cause__ else eg)
            error_msg = error_mapping.get(
                error_type, f"Unexpected error: {error_type.__name__}"
            )
            logger.exception(error_msg)
            await self.view.send_error(ctx, error_msg)

    @interactions.message_context_menu(name="Message in Lawsuit")
    async def message_management_menu(
        self, ctx: interactions.ContextMenuContext
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                permission_task = asyncio.create_task(user_is_judge(ctx))
                case_id = self.find_case_number_by_channel_id(ctx.channel_id)

                if not (await permission_task and case_id):
                    await self.view.send_error(
                        ctx,
                        (
                            "Only judges can manage messages."
                            if await permission_task
                            else "This command can only be used in case threads."
                        ),
                    )
                    return

                components = [
                    interactions.ActionRow(
                        *self.view.create_message_management_buttons(ctx.target.id)
                    )
                ]

                log_data = dict(
                    case_id=case_id, message_id=ctx.target.id, user_id=ctx.author.id
                )

                send_task = asyncio.create_task(
                    ctx.send("Select an action:", components=components, ephemeral=True)
                )

                logger.info("Message management menu opened", extra=log_data)

                await asyncio.wait([send_task], return_when=asyncio.FIRST_COMPLETED)

        except* asyncio.TimeoutError:
            await self.view.send_error(ctx, "Operation timed out")
        except* Exception as eg:
            logger.exception(
                "Failed to open message management menu",
                extra={"message_id": ctx.target.id},
                exc_info=eg,
            )
            await self.view.send_error(
                ctx, "Failed to load message management options."
            )
            raise

    @interactions.user_context_menu(name="User in Lawsuit")
    async def user_management_menu(self, ctx: interactions.ContextMenuContext) -> None:
        try:
            async with asyncio.timeout(5.0):
                permission_task = asyncio.create_task(user_is_judge(ctx))
                case_id = self.find_case_number_by_channel_id(ctx.channel_id)

                if not (await permission_task and case_id):
                    await self.view.send_error(
                        ctx,
                        (
                            "Only judges can manage users."
                            if await permission_task
                            else "This command can only be used in case threads."
                        ),
                    )
                    return

                log_data = {
                    "case_id": case_id,
                    "target_user_id": ctx.target.id,
                    "user_id": ctx.author.id,
                }

                send_task = asyncio.create_task(
                    ctx.send(
                        "Select an action:",
                        components=[
                            interactions.ActionRow(
                                *self.view.create_user_management_buttons(ctx.target.id)
                            )
                        ],
                        ephemeral=True,
                    )
                )

                logger.info("User management menu opened", extra=log_data)

                await asyncio.wait([send_task], return_when=asyncio.FIRST_COMPLETED)

        except* asyncio.TimeoutError:
            await self.view.send_error(ctx, "Operation timed out")
        except* Exception as eg:
            logger.exception(
                "Failed to open user management menu",
                extra={"target_user_id": ctx.target.id},
                exc_info=eg,
            )
            await self.view.send_error(ctx, "Failed to load user management options.")
            raise

    # Service functions

    async def process_case_action(
        self,
        ctx: interactions.ComponentContext,
        action: CaseAction,
        case_id: str,
    ) -> None:
        logger.info(
            "Initiating handling of case action '%s' for case ID '%s'.", action, case_id
        )

        try:
            async with asyncio.TaskGroup() as tg:
                case_task = tg.create_task(self.repo.get_case_data(case_id))
                guild_task = tg.create_task(self.bot.fetch_guild(self.config.GUILD_ID))
                case, guild = await case_task, await guild_task

            if not case:
                logger.warning("Case ID '%s' not found.", case_id)
                await self.view.send_error(ctx, "Case not found.")
                return

            member = await guild.fetch_member(ctx.author.id)
            logger.debug("Fetched member '%s' from guild '%s'.", member.id, guild.id)

            handler = self.case_handlers.get(action)
            if not handler:
                logger.error("No handler found for action '%s'.", action)
                await self.view.send_error(ctx, "Invalid action specified.")
                return

            async with asyncio.timeout(5.0):
                await handler(ctx, case_id, case, member)  # type: ignore
                logger.info(
                    "Successfully executed handler for action '%s' on case '%s'",
                    action,
                    case_id,
                )

        except* (asyncio.TimeoutError, asyncio.CancelledError) as eg:
            logger.error("Timeout while processing case ID '%s': %s", case_id, eg)
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
        except* Exception as eg:
            logger.exception(
                "Unexpected error in process_case_action for case ID '%s': %s",
                case_id,
                eg,
            )
            await self.view.send_error(
                ctx, f"An unexpected error occurred: {type(eg).__name__}"
            )

    async def handle_file_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        user: Union[interactions.Member, interactions.User],
    ) -> None:
        logger.info(
            "Processing file case request for case ID '%s' by user '%s'.",
            case_id,
            user.id,
        )

        try:
            async with asyncio.timeout(5.0), asyncio.TaskGroup() as tg:
                judge_check_task = tg.create_task(
                    user_is_judge(interactions.BaseContext(user))
                )
                judge_check_result = await judge_check_task

                if not judge_check_result:
                    logger.warning(
                        "User '%s' lacks judge role for case '%s'.",
                        user.id,
                        case_id,
                        extra={"user_id": user.id, "case_id": case_id},
                    )
                    await self.view.send_error(ctx, "Insufficient permissions.")
                    return

                add_judge_task = tg.create_task(self.add_judge_to_case(ctx, case_id))
                await add_judge_task

                logger.info(
                    "User '%s' assigned as judge to case '%s'.",
                    user.id,
                    case_id,
                    extra={"user_id": user.id, "case_id": case_id, "success": True},
                )

        except* (asyncio.TimeoutError, asyncio.CancelledError) as eg:
            logger.error(
                "Timeout in file case request for case ID '%s': %s",
                case_id,
                eg,
                extra={
                    "case_id": case_id,
                    "error": str(eg),
                    "error_type": type(eg).__name__,
                },
            )
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
            raise eg.derive([eg.exceptions[0]])
        except* Exception as eg:
            logger.exception(
                "Error handling file case request for case ID '%s': %s",
                case_id,
                eg,
                extra={
                    "case_id": case_id,
                    "error": str(eg),
                    "error_type": type(eg).__name__,
                },
            )
            await self.view.send_error(
                ctx,
                f"An error occurred while processing your request: {type(eg).__name__}",
            )
            raise eg.derive([eg.exceptions[0]])

    async def handle_case_closure(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        member: interactions.Member,
        action: CaseAction,
    ) -> None:
        logger.info(
            "Initiating closure of case ID '%s' with action '%s' by user '%s'.",
            case_id,
            action,
            ctx.author.id,
        )

        try:
            if not await self.user_has_permission_for_closure(
                ctx.author.id, case_id, member
            ):
                logger.warning(
                    "User '%s' lacks permissions to close case '%s'.",
                    ctx.author.id,
                    case_id,
                )
                await self.view.send_error(ctx, "Insufficient permissions.")
                return

            action_name = action.value.capitalize()
            case_status = CaseStatus(action.value)

            async with asyncio.TaskGroup() as tg:
                [
                    tg.create_task(coro)
                    for coro in (
                        self.update_case_status(case_id, case, case_status),
                        self.notify_case_closure(ctx, case_id, action_name),
                        self.enqueue_case_file_save(case_id, case),
                    )
                ]

            logger.info(
                "Successfully closed case ID '%s' with action '%s' by user '%s'.",
                case_id,
                action_name,
                ctx.author.id,
            )

        except* Exception as eg:
            logger.exception(
                "Error during case closure for case ID '%s': %s", case_id, eg
            )
            await self.view.send_error(
                ctx, f"Failed to close the case: {type(eg).__name__}"
            )

    async def update_case_status(
        self, case_id: str, case: Data, new_status: CaseStatus
    ) -> None:
        logger.debug("Updating status of case ID '%s' to '%s'.", case_id, new_status)
        case.status = new_status

        thread_ids = {tid for tid in (case.thread_id, case.trial_thread_id) if tid}
        if not thread_ids:
            return

        try:
            tasks = tuple(
                asyncio.create_task(self.archive_thread(thread_id), name=str(thread_id))
                for thread_id in thread_ids
            )

            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.ALL_COMPLETED, timeout=5.0
            )

            for task in done:
                thread_id = task.get_name()
                try:
                    await task
                    logger.info("Successfully archived thread %s", thread_id)
                except Exception as e:
                    logger.error(
                        "Failed to archive thread %s: %s", thread_id, e, exc_info=True
                    )

            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except* Exception as eg:
            logger.exception(
                "Error archiving threads for case ID '%s': %s", case_id, eg
            )

    async def notify_case_closure(
        self, ctx: interactions.ComponentContext, case_id: str, action_name: str
    ) -> None:
        notification = f"Case number {case_id} has been {action_name} by {ctx.author.display_name}."
        logger.debug("Sending closure notification for case ID '%s'.", case_id)

        try:
            async with asyncio.timeout(5.0), contextlib.AsyncExitStack() as stack:
                tasks = (
                    asyncio.create_task(
                        self.notify_case_participants(case_id, notification),
                        name=f"notify_participants_{case_id}",
                    ),
                    asyncio.create_task(
                        self.view.send_success(
                            ctx, f"Case number {case_id} successfully {action_name}."
                        ),
                        name=f"send_success_{case_id}",
                    ),
                )

                for task in tasks:
                    stack.push_async_callback(
                        lambda _: asyncio.create_task(task.cancel())
                    )

                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )

                await next(iter(done))
                await tasks[1]

            logger.info(
                "Successfully notified participants and user about closure of case ID '%s'.",
                case_id,
            )
        except* asyncio.TimeoutError:
            logger.error("Timeout notifying about closure of case ID '%s'", case_id)
            await self.view.send_error(ctx, "Operation timed out")
            raise
        except* Exception as eg:
            logger.exception(
                "Error notifying about closure of case ID '%s': %s", case_id, eg
            )
            await self.view.send_error(
                ctx, f"Failed to notify participants: {eg.__class__.__name__}"
            )

    async def handle_accept_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
    ) -> None:
        logger.info(
            "Handling acceptance of case ID '%s' by user '%s'.", case_id, ctx.author.id
        )

        try:
            async with asyncio.timeout(5.0):
                if not await self.is_user_assigned_to_case(
                    ctx.author.id, case_id, role="judges"
                ):
                    logger.warning(
                        "User '%s' is not assigned to case ID '%s'.",
                        ctx.author.id,
                        case_id,
                    )
                    await self.view.send_error(ctx, "Insufficient permissions.")
                    return

                async with contextlib.AsyncExitStack():
                    embed_task = asyncio.create_task(
                        self.view.generate_embed(title="Public or Private Trial")
                    )
                    buttons = self.view.create_trial_privacy_buttons(case_id)

                    embed = await embed_task

                    await ctx.send(
                        embeds=[embed],
                        components=[interactions.ActionRow(*buttons)],
                        allowed_mentions=interactions.AllowedMentions.none(),
                    )

                    logger.info(
                        "Sent trial privacy options for case ID '%s' to user '%s'.",
                        case_id,
                        ctx.author.id,
                    )

        except* asyncio.TimeoutError:
            logger.error(
                "Timeout handling accept case request for case ID '%s'", case_id
            )
            await self.view.send_error(ctx, "Operation timed out")
            raise
        except* Exception as eg:
            logger.exception(
                "Error handling accept case request for case ID '%s': %s",
                case_id,
                eg,
            )
            await self.view.send_error(
                ctx, f"Failed to process request: {type(eg).__name__}"
            )

    async def end_trial(self, ctx: interactions.ComponentContext) -> None:
        try:
            async with asyncio.timeout(5.0):
                if match := re.match(
                    r"^end_trial_([a-f0-9]+)$", ctx.custom_id, re.IGNORECASE
                ):
                    case_id = match[1]
                    logger.info(
                        "End trial initiated for case ID '%s' by user '%s'.",
                        case_id,
                        ctx.author.id,
                        extra={"custom_id": ctx.custom_id},
                    )
                    await self.process_case_action(ctx, CaseAction.CLOSE, case_id)
                    return

                logger.warning(
                    "Invalid custom ID format",
                    extra={
                        "custom_id": ctx.custom_id,
                        "user_id": ctx.author.id,
                        "channel_id": ctx.channel_id,
                    },
                )
                await self.view.send_error(ctx, "Invalid interaction data.")

        except* asyncio.TimeoutError as te:
            logger.error("End trial operation timed out", exc_info=te)
            await self.view.send_error(ctx, "Operation timed out")
            raise
        except* Exception as e:
            logger.exception("Critical error in end_trial", exc_info=e)
            await self.view.send_error(ctx, f"Internal error: {type(e).__name__}")
            raise

    # Thread functions

    @staticmethod
    async def add_members_to_thread(
        thread: interactions.ThreadChannel, member_ids: FrozenSet[int]
    ) -> None:
        async def _add_member(member_id: int) -> None:
            try:
                async with asyncio.timeout(5.0):
                    await thread.add_member(await thread.guild.fetch_member(member_id))
                    logger.debug(
                        "Member added successfully",
                        extra={
                            "member_id": member_id,
                            "thread_id": thread.id,
                            "guild_id": thread.guild.id,
                        },
                    )
            except* Exception as eg:
                logger.exception(
                    "Critical error adding member",
                    extra={
                        "member_id": member_id,
                        "thread_id": thread.id,
                        "error": str(eg),
                    },
                )

        async with asyncio.TaskGroup() as tg:
            for mid in member_ids:
                tg.create_task(_add_member(mid))

    async def add_judge_to_case(
        self, ctx: interactions.ComponentContext, case_id: str
    ) -> None:
        logger.info(
            "Adding judge to case",
            extra={"case_id": case_id, "judge_id": ctx.author.id},
        )

        try:
            async with self.repo.case_lock_manager(case_id) as locked_case:
                if not locked_case:
                    await self.view.send_error(ctx, "Case not found")
                    return

                if len(locked_case.judges) >= self.config.MAX_JUDGES_PER_LAWSUIT:
                    await self.view.send_error(
                        ctx,
                        f"Maximum judges ({self.config.MAX_JUDGES_PER_LAWSUIT}) reached",
                    )
                    return

                if ctx.author.id in locked_case.judges:
                    await self.view.send_error(ctx, "Already assigned as judge")
                    return

                new_judges = frozenset({*locked_case.judges, ctx.author.id})
                notification = (
                    f"{ctx.author.display_name} assigned as judge to Case #{case_id}"
                )

                async with asyncio.TaskGroup() as tg:
                    tasks = [
                        tg.create_task(
                            self.repo.update_case(case_id, judges=new_judges)
                        ),
                        tg.create_task(
                            self.notify_case_participants(case_id, notification)
                        ),
                        tg.create_task(
                            self.update_thread_permissions(case_id, ctx.author.id)
                        ),
                        tg.create_task(
                            self.view.send_success(
                                ctx, f"Added as judge to Case #{case_id}"
                            )
                        ),
                    ]
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        except* ValueError as eg:
            logger.error(
                "Validation error",
                extra={"case_id": case_id, "error": str(eg)},
            )
            await self.view.send_error(ctx, str(eg))
        except* Exception as eg:
            logger.exception(
                "Critical error",
                extra={"case_id": case_id, "error": str(eg)},
            )
            await self.view.send_error(ctx, "Critical error occurred")
            raise

    async def update_thread_permissions(self, case_id: str, judge_id: int) -> None:
        try:
            async with asyncio.timeout(5.0):
                case = await self.repo.get_case_data(case_id)

                permissions = (
                    interactions.Permissions.VIEW_CHANNEL
                    | interactions.Permissions.SEND_MESSAGES
                )

                permission_overwrite = interactions.PermissionOverwrite(
                    id=interactions.Snowflake(judge_id),
                    allow=permissions,
                    deny=None,
                    type=interactions.OverwriteType.MEMBER,
                )

                thread_ids = {
                    tid
                    for tid in ("thread_id", "trial_thread_id")
                    if hasattr(case, tid) and getattr(case, tid) is not None
                }

                if not thread_ids:
                    logger.warning("No threads found", extra={"case_id": case_id})
                    return

                async with asyncio.TaskGroup() as tg:
                    channels = [await self.bot.fetch_channel(tid) for tid in thread_ids]
                    [
                        tg.create_task(
                            channel.edit_permission(
                                permission_overwrite,
                                reason=f"Adding judge {judge_id} to case {case_id}",
                            )
                        )
                        for channel in channels
                    ]

                    for tid in thread_ids:
                        logger.debug(
                            "Thread permissions updated",
                            extra={"thread_id": tid, "judge_id": judge_id},
                        )

        except* Exception as eg:
            logger.exception(
                "Critical permission update error",
                extra={"case_id": case_id, "judge_id": judge_id, "error": str(eg)},
            )
            raise

    # User operations

    async def process_mute_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        mute: bool,
    ) -> None:
        action_str = "muted"[: (5 + (not mute) * 2)]

        async with self.repo.case_lock_manager(case_id):
            if (case := await self.repo.get_case_data(case_id)) is None:
                raise ValueError(f"Case {case_id} not found")

            mute_list: frozenset[int] = getattr(case, "mute_list", frozenset())
            if (target_id in mute_list) == mute:
                await self.view.send_error(
                    ctx, f"User <@{target_id}> is already {action_str}."
                )
                return

            updated_mute_list = (
                mute_list | {target_id} if mute else mute_list - {target_id}
            )

            notification = f"User <@{target_id}> has been {action_str}"
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self.repo.update_case(
                        case_id, mute_list=tuple(sorted(updated_mute_list))
                    )
                )
                tg.create_task(self.view.send_success(ctx, f"{notification}."))
                tg.create_task(
                    self.notify_case_participants(
                        case_id,
                        f"{notification} by <@{ctx.author.id}>.",
                    )
                )

    async def process_message_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        action_type: MessageAction,
    ) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                channel = await anext(
                    aiter(
                        [await tg.create_task(self.bot.fetch_channel(ctx.channel_id))][
                            0
                        ]
                    )
                )
                if not channel:
                    raise ValueError(f"Channel {ctx.channel_id} not found")
                message = await anext(
                    aiter([await tg.create_task(channel.fetch_message(target_id))][0])
                )
                if not message:
                    raise ValueError(f"Message {target_id} not found")

            action_map = dict(
                zip(
                    (MessageAction.PIN.value, MessageAction.DELETE.value),
                    map(
                        lambda x: (getattr(message, x[0]), x[1]),
                        [("pin", "ned"), ("delete", "d")],
                    ),
                )
            )

            try:
                action_func, suffix = action_map[action_type.value]
            except KeyError:
                raise ValueError(f"Invalid action_type: {action_type}")
            action_str = f"{action_type}{suffix}"
            await action_func()

            async with asyncio.TaskGroup() as tg:
                [
                    tg.create_task(coro)
                    for coro in (
                        self.view.send_success(ctx, f"Message has been {action_str}."),
                        self.notify_case_participants(
                            case_id,
                            f"A message has been {action_str} by <@{ctx.author.id}> in case {case_id}.",
                        ),
                    )
                ]

        except NotFound:
            logger.warning(
                "Message not found", extra={"message_id": target_id, "case_id": case_id}
            )
            await self.view.send_error(ctx, f"Message not found in case {case_id}.")
        except Forbidden:
            logger.warning(
                "Permission denied",
                extra={
                    "action": action_type,
                    "message_id": target_id,
                    "case_id": case_id,
                },
            )
            await self.view.send_error(
                ctx, f"Insufficient permissions to {action_type} the message."
            )
        except HTTPException as e:
            logger.error(
                f"HTTP error during message {action_type}",
                extra={"error": str(e), "message_id": target_id, "case_id": case_id},
            )
            await self.view.send_error(
                ctx, f"An error occurred while {action_type}ing the message: {str(e)}"
            )

    # Notification functions

    async def notify_evidence_decision(
        self, user_id: int, case_id: str, decision: str
    ) -> None:
        embed = await self.view.generate_embed(
            title="Evidence Decision",
            description=f"Your evidence for **Case {case_id}** has been **{decision}**.",
        )
        await self.send_dm_with_fallback(user_id, embed=embed)

    async def notify_case_participants(self, case_id: str, message: str) -> None:
        if case := await self.repo.get_case_data(case_id):
            participants: frozenset[int] = frozenset(
                {case.plaintiff_id, case.defendant_id} | case.judges
            )
            await self.notify_users(
                user_ids=participants,
                notification_func=functools.partial(
                    self.send_dm_with_fallback,
                    embed=await self.view.generate_embed(
                        title=f"Case {case_id} Update", description=message
                    ),
                ),
            )
        else:
            logger.warning(
                "Case %s not found when attempting to notify participants", case_id
            )

    async def notify_judges_of_new_evidence(self, case_id: str, user_id: int) -> None:
        if not (case := await self.repo.get_case_data(case_id)):
            logger.warning("Case %s not found for evidence notification", case_id)
            return

        embed_task = self.view.generate_embed(
            title="Evidence Submission",
            description=(
                f"New evidence has been submitted for **Case {case_id}**. "
                "Please review and decide whether to make it public."
            ),
        )
        buttons = self.view.create_evidence_action_buttons(case_id, user_id)

        async with asyncio.TaskGroup() as tg:
            embed = await tg.create_task(embed_task)
            await tg.create_task(
                self.notify_users(
                    user_ids=getattr(case, "judges", frozenset()),
                    notification_func=functools.partial(
                        self.send_dm_with_fallback,
                        embed=embed,
                        components=[interactions.ActionRow(*buttons)],
                    ),
                )
            )

    async def notify_judges(self, case_id: str, guild_id: int) -> None:
        async with asyncio.TaskGroup() as tg:
            guild_task = tg.create_task(self.bot.fetch_guild(guild_id))
            case_task = tg.create_task(self.repo.get_case_data(case_id))

            guild = await guild_task
            case = await case_task

            if not (judge_role := guild.get_role(self.config.JUDGE_ROLE_ID)):
                logger.warning(
                    "Judge role with ID %s not found in guild %s",
                    self.config.JUDGE_ROLE_ID,
                    guild_id,
                )
                return

            judge_members = frozenset(
                member.id
                for member in guild.members
                if any(r.id == self.config.JUDGE_ROLE_ID for r in member.roles)
            )

            if not judge_members:
                logger.warning(
                    "No judges found with role ID %s for case %s",
                    self.config.JUDGE_ROLE_ID,
                    case_id,
                )
                return

            if not case or not case.thread_id:
                logger.warning(
                    "Case %s does not have an associated thread for notifications",
                    case_id,
                )
                return

            thread_link = f"https://discord.com/channels/{guild_id}/{case.thread_id}"

            embed_task = tg.create_task(
                self.view.generate_embed(
                    title=f"Case #{case_id} Complaint Received",
                    description=(
                        f"Please proceed to the [mediation room]({thread_link}) "
                        "to participate in mediation."
                    ),
                )
            )

            embed = await embed_task
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
                    self.send_dm_with_fallback, embed=embed, components=components
                ),
            )

    async def send_dm_with_fallback(self, user_id: int, **kwargs: Any) -> None:
        try:
            async with asyncio.timeout(5.0):
                user = await self.bot.fetch_user(user_id)
                await user.send(**kwargs)
                logger.debug(
                    "DM sent to user %s successfully",
                    user_id,
                    extra={"user_id": user_id},
                )
        except* asyncio.TimeoutError:
            logger.warning(
                "DM send timed out", extra={"user_id": user_id}, exc_info=True
            )
        except* HTTPException as http_exc:
            logger.warning(
                "Failed to send DM",
                extra={
                    "user_id": user_id,
                    "status": getattr(http_exc, "status", None),
                    "code": getattr(http_exc, "code", None),
                },
                exc_info=True,
            )
        except* Exception as exc:
            logger.error(
                "Unexpected DM send error",
                extra={"user_id": user_id, "error_type": type(exc).__name__},
                exc_info=True,
            )

    async def notify_users(
        self,
        user_ids: frozenset[int],
        notification_func: Callable[[int], Awaitable[None]],
    ) -> None:
        async with asyncio.TaskGroup() as tg:
            tasks = tuple(
                tg.create_task(
                    self.notify_user_with_handling(user_id, notification_func),
                    name=f"notify_user_{user_id}",
                )
                for user_id in user_ids
            )
            try:
                if tasks:
                    await tasks[0]
                    if len(tasks) > 1:
                        await asyncio.wait(
                            tasks[1:],
                            return_when=asyncio.ALL_COMPLETED,
                        )
            except* Exception:
                pass

    @staticmethod
    async def notify_user_with_handling(
        user_id: int, notification_func: Callable[[int], Awaitable[None]]
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                await notification_func(user_id)
                logger.debug(
                    "Notification sent",
                    extra={"user_id": user_id, "success": True},
                )
        except* asyncio.TimeoutError:
            logger.warning(
                "Notification timed out",
                extra={"user_id": user_id, "success": False},
            )
        except* HTTPException as http_exc:
            logger.error(
                "HTTP error in notification",
                extra={
                    "user_id": user_id,
                    "success": False,
                    "status": getattr(http_exc, "status", None),
                    "code": getattr(http_exc, "code", None),
                },
                exc_info=True,
            )
        except* Exception as exc:
            logger.error(
                "Unexpected notification error",
                extra={
                    "user_id": user_id,
                    "success": False,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )

    #  Role functions

    @manage_user_role.autocomplete("role")
    async def role_autocomplete(self, ctx: interactions.AutocompleteContext) -> None:
        input_text = memoryview(ctx.input_text.encode()).tobytes().lower()
        choices = tuple(
            {"name": role.value, "value": role.name}
            for role in CaseRole
            if input_text in memoryview(role.value.encode()).tobytes().lower()
        )[:25]
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
                if not (case_data := await self.repo.get_case_data(case_id)):
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

                match action:
                    case RoleAction.ASSIGN:
                        if user.id in users_with_role:
                            raise ValueError(
                                f"User <@{user.id}> already has the role `{role_enum.name}`."
                            )
                        users_with_role.add(user.id)
                        action_str, prep = "Assigned", "to"
                    case RoleAction.REVOKE:
                        if user.id not in users_with_role:
                            raise ValueError(
                                f"User <@{user.id}> does not possess the role `{role_enum.name}`."
                            )
                        users_with_role.remove(user.id)
                        action_str, prep = "Revoked", "from"
                    case _:
                        raise ValueError(f"Invalid action: `{action}`.")

                await self.repo.update_case(case_id, roles=roles)

                notification = (
                    f"<@{ctx.author.id}> has {action_str.lower()} the role of "
                    f"`{role_enum.name}` {prep} <@{user.id}>."
                )

                success_msg = (
                    f"Successfully {action_str.lower()} role `{role_enum.name}` "
                    f"{prep} <@{user.id}>."
                )

                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.view.send_success(ctx, success_msg))
                    tg.create_task(self.notify_case_participants(case_id, notification))

        except (ValueError, PermissionError) as e:
            logger.warning("Role management error in case '%s': %s", case_id, e)
            await self.view.send_error(ctx, str(e))
        except asyncio.TimeoutError:
            logger.error("Timeout acquiring lock for case '%s'", case_id)
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
        except Exception as e:
            logger.exception(
                "Unexpected error in manage_case_role for case '%s': %s",
                case_id,
                e,
            )
            await self.view.send_error(
                ctx, "An unexpected error occurred while processing your request."
            )

    # Evidence functions

    @staticmethod
    async def fetch_attachment_as_file(
        attachment: Dict[str, str]
    ) -> Optional[interactions.File]:
        try:
            url, filename = attachment["url"], attachment["filename"]
        except KeyError:
            logger.error(
                "Invalid attachment data: missing required fields",
                extra={"attachment": attachment},
            )
            return None

        try:
            async with (
                asyncio.timeout(10),
                aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10),
                    raise_for_status=True,
                    connector=aiohttp.TCPConnector(ssl=False),
                ) as session,
                session.get(url) as response,
            ):
                content = await response.content.read()
                return (
                    interactions.File(
                        file=io.BytesIO(content),
                        file_name=filename,
                        description=f"Evidence attachment for {filename}",
                    )
                    if content
                    else None
                )

        except* (asyncio.TimeoutError, aiohttp.ClientError, OSError, ValueError) as eg:
            error_type = type(eg.exceptions[0]).__name__
            logger.error(
                f"{error_type} fetching attachment",
                extra={
                    "url": url,
                    "filename": filename,
                    "error": str(eg.exceptions[0]),
                },
            )
        except* Exception as eg:
            logger.exception(
                "Unexpected error fetching attachment",
                extra={
                    "url": url,
                    "filename": filename,
                    "error": str(eg.exceptions[0]),
                },
            )
        return None

    async def process_judge_evidence(self, ctx: interactions.ComponentContext) -> None:
        pattern = re.compile(r"^(?:approve|reject)_evidence_[a-f0-9]+_\d+$")
        if not pattern.match(ctx.custom_id):
            logger.warning(
                "Invalid evidence action format", extra={"custom_id": ctx.custom_id}
            )
            await self.view.send_error(ctx, "Invalid evidence action format")
            return

        try:
            action, case_id, user_id = ctx.custom_id.split("_")[::2]
            await self.process_judge_evidence_decision(
                ctx=ctx,
                action=EvidenceAction(action.upper()),
                case_id=case_id,
                user_id=int(user_id),
            )
        except (ValueError, IndexError):
            logger.warning(
                "Invalid user ID in evidence action",
                extra={"custom_id": ctx.custom_id},
            )
            await self.view.send_error(ctx, "Invalid user ID format")

    async def process_judge_evidence_decision(
        self,
        ctx: interactions.ComponentContext,
        action: EvidenceAction,
        case_id: str,
        user_id: int,
    ) -> None:
        async with self.case_lock_manager(case_id) as case:
            if not (
                case
                and (
                    evidence := next(
                        (e for e in case.evidence_queue if e.user_id == user_id), None
                    )
                )
            ):
                logger.error(
                    "Case or evidence not found",
                    extra={"case_id": case_id, "user_id": user_id},
                )
                await self.view.send_error(
                    ctx, f"{'Case' if not case else 'Evidence'} not found"
                )
                return

            evidence.state = action
            evidence_list_attr = f"evidence_{action.name.lower()}"

            setattr(
                case,
                evidence_list_attr,
                [*getattr(case, evidence_list_attr, []), evidence],
            )
            case.evidence_queue = [
                e for e in case.evidence_queue if e.user_id != user_id
            ]

            success = True
            error_msg = None
            try:
                async with asyncio.timeout(5.0):
                    await self.repo.update_case(
                        case_id,
                        evidence_queue=case.evidence_queue,
                        **{evidence_list_attr: getattr(case, evidence_list_attr)},
                    )
            except* (asyncio.TimeoutError, ValueError) as eg:
                logger.error(
                    f"{type(eg.exceptions[0]).__name__} updating case evidence",
                    extra={
                        "case_id": case_id,
                        "user_id": user_id,
                        "error": str(eg.exceptions[0]),
                    },
                )
                success = False
                error_msg = "Failed to update evidence state"
            except* Exception as eg:
                logger.exception(
                    "Unexpected error updating case evidence",
                    extra={
                        "case_id": case_id,
                        "user_id": user_id,
                        "error": str(eg.exceptions[0]),
                    },
                )
                success = False
                error_msg = "Failed to update evidence state"

            if not success:
                await self.view.send_error(ctx, error_msg)
                return

            notification = f"<@{user_id}>'s evidence has been {action.name.lower()} by <@{ctx.author.id}>."

            async with asyncio.TaskGroup() as tg:
                [
                    tg.create_task(coro)
                    for coro in (
                        self.notify_evidence_decision(
                            user_id, case_id, action.name.lower()
                        ),
                        self.notify_case_participants(case_id, notification),
                        self.handle_evidence_finalization(ctx, case, evidence, action),
                        self.view.send_success(
                            ctx, f"Evidence {action.name.lower()} successfully"
                        ),
                    )
                ]

    async def handle_evidence_finalization(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
        new_state: EvidenceAction,
    ) -> None:
        await (
            self.publish_approved_evidence(ctx, case, evidence)
            if new_state is EvidenceAction.APPROVED
            else self.handle_rejected_evidence(ctx, case, evidence)
        )

    async def publish_approved_evidence(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
    ) -> None:
        if not case.trial_thread_id:
            await self.view.send_success(
                ctx,
                "Evidence approved and queued for trial start",
            )
            logger.info(
                "Evidence approved pre-trial",
                extra={"case_id": case.id, "user_id": evidence.user_id},
            )
            return

        try:
            async with asyncio.timeout(10.0):
                trial_thread = await self.bot.fetch_channel(case.trial_thread_id)
                content = f"Evidence from <@{evidence.user_id}>:\n{evidence.content}"

                async with asyncio.TaskGroup() as tg:
                    files = [
                        file
                        for file in [
                            await tg.create_task(
                                self.fetch_attachment_as_file(
                                    {"url": url, "filename": filename}
                                )
                            )
                            for url, filename in (
                                (att.url, att.filename) for att in evidence.attachments
                            )
                        ]
                        if file is not None
                    ]

                await trial_thread.send(content=content, files=files)
                await self.view.send_success(ctx, "Evidence published to trial thread")

        except* HTTPException as eg:
            logger.error(
                f"HTTP error publishing evidence: {eg}",
                extra={"case_id": case.id, "user_id": evidence.user_id},
                exc_info=True,
            )
            await self.view.send_error(
                ctx, "Failed to publish evidence due to HTTP error"
            )
        except* asyncio.TimeoutError:
            logger.error(
                "Timeout publishing evidence",
                extra={"case_id": case.id, "user_id": evidence.user_id},
            )
            await self.view.send_error(
                ctx, "Failed to publish evidence - operation timed out"
            )
        except* OSError as eg:
            logger.error(
                "OS error publishing evidence",
                extra={
                    "case_id": case.id,
                    "user_id": evidence.user_id,
                    "error": str(eg),
                },
            )
            await self.view.send_error(
                ctx, "Failed to publish evidence due to system error"
            )
        except* Exception:
            logger.exception(
                "Unexpected error publishing evidence",
                extra={"case_id": case.id, "user_id": evidence.user_id},
            )
            await self.view.send_error(
                ctx, "Failed to publish evidence due to an unexpected error"
            )

    async def handle_rejected_evidence(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                user = await self.bot.fetch_user(evidence.user_id)
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(user.send("Your evidence has been rejected")),
                    tg.create_task(
                        self.view.send_success(ctx, "Evidence rejection processed")
                    ),

        except* HTTPException as eg:
            match eg:
                case HTTPException(status=403):
                    logger.warning(
                        "Cannot DM user - messages disabled",
                        extra={"user_id": evidence.user_id},
                    )
                case HTTPException():
                    logger.error(
                        "HTTP error sending rejection",
                        extra={"user_id": evidence.user_id},
                        exc_info=True,
                    )
            await self.view.send_error(ctx, "Failed to notify user of rejection")

        except* Exception:
            logger.exception(
                "Failed to handle rejected evidence",
                extra={"case_id": case.id, "user_id": evidence.user_id},
            )
            await self.view.send_error(ctx, "Failed to process evidence rejection")

    async def handle_direct_message_evidence(
        self,
        message: interactions.Message,
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                case_id = await self.get_user_active_case_number(message.author.id)
                if not case_id:
                    await message.author.send(
                        "No active cases found for evidence submission"
                    )
                    logger.info(
                        "Evidence submission without active case",
                        extra={"user_id": message.author.id},
                    )
                    return

                evidence = self.create_evidence_dict(message, self.config.MAX_FILE_SIZE)
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self.process_evidence(case_id, evidence))
                    tg.create_task(
                        message.author.send("Evidence processed successfully")
                    )

        except* Exception as eg:
            match type(eg):
                case t if t is ValueError:
                    await message.author.send(str(eg))
                    logger.warning(
                        "Invalid evidence submission",
                        extra={"user_id": message.author.id},
                    )
                case t if t is HTTPException and getattr(eg, "status", None) == 403:
                    logger.error(
                        "Cannot DM user - messages disabled",
                        extra={"user_id": message.author.id},
                    )
                case _:
                    logger.exception(
                        "Failed to process evidence submission",
                        extra={"user_id": message.author.id},
                    )

    async def handle_channel_message(
        self,
        message: interactions.Message,
    ) -> None:
        if not (channel_id := message.channel.id) or not (
            case_id := self.find_case_number_by_channel_id(channel_id)
        ):
            logger.debug("No case for channel", extra={"channel_id": channel_id})
            return

        try:
            async with asyncio.timeout(5.0):
                match await self.repo.get_case_data(case_id):
                    case None:
                        return
                    case case_data if (author_id := message.author.id) in (
                        getattr(case_data, "mute_list", [])
                    ):
                        await message.delete()
                        logger.info(
                            "Deleted muted user message",
                            extra={"user_id": author_id, "channel_id": channel_id},
                        )
                    case _:
                        return

        except* Forbidden:
            logger.warning(
                "Cannot delete message - insufficient permissions",
                extra={"channel_id": channel_id},
            )
        except* Exception:
            logger.exception(
                "Failed to handle channel message",
                extra={
                    "channel_id": channel_id,
                    "message_id": message.id,
                },
            )

    def create_evidence_dict(
        self,
        message: interactions.Message,
        max_file_size: int,
    ) -> Evidence:
        sanitized_content = self.sanitize_user_input(message.content)
        attachments = tuple(
            Attachment(
                url=a.url,
                filename=self.sanitize_user_input(a.filename),
                content_type=a.content_type,
            )
            for a in message.attachments
            if self.is_valid_attachment(a, max_file_size)
        )

        evidence = Evidence(
            user_id=message.author.id,
            content=sanitized_content,
            attachments=attachments,
            message_id=message.id,
            timestamp=message.created_at,
            state=EvidenceAction.PENDING,
        )

        logger.debug(
            "Created evidence record",
            extra={
                "message_id": message.id,
                "user_id": message.author.id,
                "attachment_count": len(attachments),
            },
        )
        return evidence

    async def process_evidence(
        self,
        case_id: str,
        evidence: Evidence,
    ) -> None:
        async with contextlib.AsyncExitStack() as stack:
            try:
                async with asyncio.TaskGroup() as tg:
                    lock_task = tg.create_task(
                        stack.enter_async_context(self.case_lock_manager(case_id))
                    )
                    case_task = tg.create_task(self.repo.get_case_data(case_id))
                    await lock_task
                    case = await case_task

                if case is None:
                    raise ValueError(f"Case {case_id} not found")

                case_data = Data.model_validate(case)
                valid_statuses = {CaseStatus.FILED, CaseStatus.IN_PROGRESS}
                if case_data.status not in valid_statuses:
                    raise ValueError(f"Case {case_id} not accepting evidence")

                updated_queue = tuple([*(case_data.evidence_queue or ()), evidence])

                async with asyncio.TaskGroup() as tg:
                    update_task = tg.create_task(
                        self.repo.update_case(
                            case_id,
                            evidence_queue=updated_queue,
                        )
                    )
                    notify_task = tg.create_task(
                        self.notify_judges_of_new_evidence(
                            case_id,
                            evidence.user_id,
                        )
                    )
                    await update_task
                    try:
                        await asyncio.wait_for(notify_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        raise

            except* asyncio.TimeoutError:
                logger.warning(
                    "Judge notification timeout",
                    extra={"case_id": case_id},
                )
            except* Exception as exc:
                logger.critical(
                    "Evidence processing failed",
                    extra={"case_id": case_id, "error": str(exc)},
                    exc_info=True,
                )
                raise

    # Validation

    async def user_has_judge_role(
        self,
        user: Union[interactions.Member, interactions.User],
    ) -> bool:
        member = await self._fetch_member(user)
        return any(role.id == self.config.JUDGE_ROLE_ID for role in member.roles)

    async def _fetch_member(
        self,
        user: Union[interactions.Member, interactions.User],
    ) -> interactions.Member:
        return (
            user
            if isinstance(user, interactions.Member)
            else await self.fetch_member_from_user(user)
        )

    async def fetch_member_from_user(
        self,
        user: interactions.User,
    ) -> interactions.Member:
        async with asyncio.TaskGroup() as tg:
            guild = await tg.create_task(
                self.bot.fetch_guild(self.config.GUILD_ID).__await__()
            )
            member = await tg.create_task(guild.fetch_member(user.id).__await__())
            return member

    async def user_has_permission_for_closure(
        self,
        user_id: int,
        case_id: str,
        member: interactions.Member,
    ) -> bool:
        async with asyncio.TaskGroup() as tg:
            is_assigned = await tg.create_task(
                self.is_user_assigned_to_case(user_id, case_id, "plaintiff_id")
            )
            return is_assigned and next(
                (True for role in member.roles if role.id == self.config.JUDGE_ROLE_ID),
                False,
            )

    def is_valid_attachment(
        self,
        attachment: interactions.Attachment,
        max_size: int,
    ) -> bool:
        return not (
            attachment.content_type.startswith("image/")
            and (
                attachment.content_type not in self.config.ALLOWED_MIME_TYPES
                or attachment.size > max_size
                or not attachment.filename
                or attachment.height is None
            )
        )

    @staticmethod
    def get_role_check(
        role: str,
    ) -> Callable[[Data, int], bool]:
        return {
            "plaintiff_id": (lambda c, u: c.plaintiff_id == u).__get__(None, object),
            "defendant_id": (lambda c, u: c.defendant_id == u).__get__(None, object),
            "judges": (lambda c, u: u in c.judges).__get__(None, object),
            "witnesses": (lambda c, u: u in c.witnesses).__get__(None, object),
            "attorneys": (lambda c, u: u in c.attorneys).__get__(None, object),
        }.get(
            role,
            (lambda c, u: u in getattr(c, role, frozenset())).__get__(None, object),
        )

    async def is_user_assigned_to_case(
        self,
        user_id: int,
        case_id: str,
        role: str,
    ) -> bool:
        try:
            case = await self.repo.get_case_data(case_id)
            if case is None:
                logger.error("Case '%s' not found", case_id)
                return False

            role_check = self.get_role_check(role)
            loop = asyncio.get_running_loop()
            validated_case = Data.model_validate(case)
            return await loop.run_in_executor(
                None,
                role_check,
                validated_case,
                user_id,
            )
        except Exception:
            logger.exception(
                "Failed to check user case assignment",
                extra={"user_id": user_id, "case_id": case_id, "role": role},
            )
            return False

    async def has_ongoing_lawsuit(
        self,
        user_id: int,
    ) -> bool:
        return any(
            Data.model(case).plaintiff_id == user_id
            and Data.model_validate(case).status != CaseStatus.CLOSED
            for case in self.cached_cases.values()
        )

    async def validate_user_permissions(self, user_id: int, case_id: str) -> bool:
        try:
            if not (case := await self.repo.get_case_data(case_id)):
                logger.warning("Case %s not found during permission check", case_id)
                return False

            case_data = Data.model_validate(case)
            result = user_id in case_data.judges or user_id in {
                case_data.plaintiff_id,
                case_data.defendant_id,
            }

            logger.debug(
                "Permission check completed",
                extra={"user_id": user_id, "case_id": case_id, "result": result},
            )
            return result

        except Exception as e:
            logger.exception(
                "Permission check failed",
                extra={"user_id": user_id, "case_id": case_id, "error": str(e)},
            )
            return False

    # Helper methods

    async def archive_thread(self, thread_id: int) -> bool:
        try:
            async with asyncio.timeout(5.0):
                if not isinstance(
                    thread := await self.bot.fetch_channel(thread_id),
                    interactions.ThreadChannel,
                ):
                    logger.error(
                        "Invalid channel type",
                        extra={"thread_id": thread_id, "type": type(thread).__name__},
                    )
                    return False

                await asyncio.shield(thread.edit(archived=True, locked=True))
                logger.info(
                    "Thread archived successfully", extra={"thread_id": thread_id}
                )
                return True

        except* (asyncio.TimeoutError, HTTPException) as eg:
            logger.error(
                "Failed to archive thread",
                extra={"thread_id": thread_id, "error": str(eg)},
            )
        except* Exception:
            logger.exception(
                "Unexpected error archiving thread", extra={"thread_id": thread_id}
            )
        return False

    @staticmethod
    def sanitize_user_input(text: str, *, max_length: int = 2000) -> str:
        _WHITESPACE_PATTERN: re.Pattern = re.compile(r"\s+", re.UNICODE)
        _ALLOWED_TAGS: frozenset[str] = frozenset()
        _ALLOWED_ATTRS: Dict[str, frozenset[str]] = {}
        _CLEANER = functools.lru_cache(maxsize=1024)(
            functools.partial(
                bleach.clean,
                tags=_ALLOWED_TAGS,
                attributes=_ALLOWED_ATTRS,
                strip=True,
                protocols=bleach.ALLOWED_PROTOCOLS,
            )
        )

        return next(
            itertools.islice(
                (_WHITESPACE_PATTERN.sub(" ", _CLEANER(text)).strip(),), 0, max_length
            )
        )

    async def delete_thread_created_message(
        self,
        channel_id: int,
        thread: interactions.ThreadChannel,
    ) -> bool:
        try:
            async with asyncio.timeout(5.0), contextlib.AsyncExitStack():
                parent_channel = await self.bot.fetch_channel(channel_id)
                if not isinstance(parent_channel, interactions.GuildText):
                    logger.error(
                        "Invalid parent channel type",
                        extra=dict(
                            channel_id=channel_id, type=type(parent_channel).__name__
                        ),
                    )
                    return False

                messages = parent_channel.history(limit=50).__aiter__()
                while True:
                    try:
                        message = await anext(messages)
                        if (
                            message.type == interactions.MessageType.THREAD_CREATED
                            and message.thread_id == thread.id
                        ):
                            await asyncio.shield(message.delete())
                            logger.info(
                                "Thread creation message deleted",
                                extra={"thread_id": thread.id},
                            )
                            return True
                    except StopAsyncIteration:
                        break

                logger.warning(
                    "Thread creation message not found",
                    extra={"thread_id": thread.id},
                )

        except* (
            asyncio.TimeoutError,
            HTTPException,
            Forbidden,
        ) as eg:
            logger.error(
                "Failed to delete thread message",
                extra={"thread_id": thread.id, "error": str(eg)},
            )
        except* Exception:
            logger.exception(
                "Unexpected error deleting thread message",
                extra={"thread_id": thread.id},
            )
        return False

    async def initialize_chat_thread(self, ctx: interactions.SlashContext) -> bool:
        try:
            async with asyncio.timeout(10.0) as timeout_ctx:
                channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                if not isinstance(channel, interactions.GuildText):
                    logger.error("Invalid courtroom channel type")
                    return False

                async with asyncio.TaskGroup() as tg:
                    thread = await tg.create_task(
                        self.get_or_create_chat_thread(channel)
                    )
                    embed = await self.create_chat_room_setup_embed(thread)

                    try:
                        await tg.create_task(
                            self.view.send_success(ctx, embed.description)
                        )
                        logger.info("Chat thread initialized successfully")
                        return True
                    except NotFound:
                        logger.warning("Interaction expired, attempting fallback")
                        await tg.create_task(ctx.send(embed=embed, ephemeral=True))
                        return True

        except* (asyncio.TimeoutError, HTTPException) as eg:
            logger.error(
                "Failed to initialize chat thread",
                extra={
                    "error": str(eg),
                    "elapsed": time.monotonic() - timeout_ctx.when(),
                },
            )
        except* Exception:
            logger.exception("Unexpected error in chat thread initialization")
            try:
                await asyncio.shield(
                    self.view.send_error(
                        ctx,
                        "Setup failed. Please try again later.",
                    )
                )
            except NotFound:
                logger.warning("Failed to send error message - interaction expired")
        return False

    async def setup_mediation_thread(self, case_id: str, case: Data) -> bool:
        try:
            async with asyncio.timeout(15.0):
                async with asyncio.TaskGroup() as tg:
                    channel_task = tg.create_task(
                        self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                    )
                    guild_task = tg.create_task(
                        self.bot.fetch_guild(self.config.GUILD_ID)
                    )
                    channel, guild = await channel_task, await guild_task

                if not guild:
                    logger.error(
                        "Guild not found", extra={"guild_id": self.config.GUILD_ID}
                    )
                    return False

                async with asyncio.TaskGroup() as tg:
                    thread_task = tg.create_task(
                        self.create_mediation_thread(channel, case_id)
                    )
                    judges_task = tg.create_task(self.get_judges(guild))
                    thread, judges = await thread_task, await judges_task

                participants = frozenset(
                    {case.plaintiff_id, case.defendant_id} | judges
                )

                async with asyncio.TaskGroup() as tg:
                    summary_task = tg.create_task(
                        self.send_case_summary(thread, case_id, case)
                    )
                    update_task = tg.create_task(
                        self.update_case_with_thread(case_id, thread.id)
                    )
                    members_task = tg.create_task(
                        self.add_members_to_thread(thread, participants)
                    )
                    await asyncio.wait(
                        (summary_task, update_task, members_task),
                        return_when=asyncio.ALL_COMPLETED,
                        timeout=10.0,
                    )

                logger.info(
                    "Mediation thread setup completed", extra={"case_id": case_id}
                )
                return True

        except Exception as e:
            logger.exception(
                f"Mediation thread setup failed: {e}", extra={"case_id": case_id}
            )
            raise

    async def send_case_summary(
        self,
        thread: interactions.ThreadChannel,
        case_id: str,
        case: Data,
    ) -> bool:
        try:
            async with asyncio.timeout(5.0), contextlib.AsyncExitStack():
                components = self.view.create_action_buttons(case_id)
                embed = await asyncio.create_task(
                    self.view.create_summary_embed(case_id, case),
                    name=f"embed_task_{case_id}",
                )

                await asyncio.shield(
                    thread.send(
                        embeds=[embed],
                        components=components,
                        allowed_mentions=interactions.AllowedMentions.none(),
                    )
                )

                logger.info(
                    "Case summary sent",
                    extra={"case_id": case_id, "thread_id": thread.id},
                    stacklevel=2,
                )
                return True

        except* (asyncio.TimeoutError, HTTPException) as eg:
            logger.exception(
                "Failed to send case summary",
                extra={"case_id": case_id, "thread_id": thread.id, "error": str(eg)},
            )
        except* Exception as eg:
            logger.critical(
                "Critical error sending case summary",
                extra={"case_id": case_id, "thread_id": thread.id, "error": str(eg)},
                exc_info=True,
            )
        return False

    async def update_case_with_thread(self, case_id: str, thread_id: int) -> bool:
        success = False
        try:
            async with asyncio.timeout(5.0):
                update_task = asyncio.create_task(
                    self.repo.update_case(case_id, thread_id=thread_id),
                    name=f"update_{case_id}_{thread_id}",
                )
                await asyncio.shield(update_task)

                logger.info(
                    "Case updated with thread",
                    extra={"case_id": case_id, "thread_id": thread_id},
                    stacklevel=2,
                )
                success = True

        except* (asyncio.TimeoutError, Exception) as eg:
            logger.exception(
                "Failed to update case with thread",
                extra={
                    "case_id": case_id,
                    "thread_id": thread_id,
                    "error": str(eg),
                    "error_type": type(eg).__name__,
                },
            )

        return success

    @staticmethod
    def sanitize_input(text: str, max_length: int = 2000) -> str:
        return (
            next(itertools.islice((re.sub(r"\s+", " ", text).strip(),), 0, max_length))
            if text
            else ""
        )

    # Initialization

    @staticmethod
    async def create_mediation_thread(
        channel: interactions.GuildText,
        case_id: str,
    ) -> interactions.ThreadChannel:
        thread_name: str = f"第{case_id}号调解室"
        try:
            async with asyncio.timeout(5.0), contextlib.AsyncExitStack():
                thread = await asyncio.shield(
                    channel.create_thread(
                        name=thread_name,
                        thread_type=interactions.ChannelType.GUILD_PRIVATE_THREAD,
                        reason=f"Mediation thread for case {case_id}",
                    )
                )
                logger.info(
                    "Created mediation thread",
                    extra={
                        "thread_id": thread.id,
                        "thread_name": thread_name,
                        "case_id": case_id,
                    },
                )
                return thread

        except* HTTPException as eg:
            logger.error(
                "Failed to create mediation thread",
                extra={
                    "thread_name": thread_name,
                    "case_id": case_id,
                    "error": str(eg),
                },
                exc_info=True,
            )
            raise

    @staticmethod
    def create_case_id(length: int = 6) -> str:
        if not (isinstance(length := int(length), int) and 0 < length <= 32):
            raise ValueError(f"Length must be between 1-32, got {length}")
        return (token := secrets.token_bytes(length)).hex()[:length]  # noqa: F841

    async def create_new_case(
        self,
        ctx: interactions.ModalContext,
        data: Dict[str, str],
        defendant_id: int,
    ) -> Tuple[Optional[str], Optional[Data]]:
        result: Tuple[Optional[str], Optional[Data]] = (None, None)
        try:
            async with asyncio.timeout(10.0), contextlib.AsyncExitStack():
                case_id = self.create_case_id()
                case = await self.create_case_data(ctx, data, defendant_id)

                await asyncio.shield(self.repo.persist_case(case_id, case))

                logger.info(
                    "Created new case",
                    extra={
                        "case_id": case_id,
                        "plaintiff_id": ctx.author.id,
                        "defendant_id": defendant_id,
                    },
                )
                result = (case_id, case)

        except* Exception:
            logger.exception(
                "Failed to create case",
                extra={
                    "plaintiff_id": ctx.author.id,
                    "defendant_id": defendant_id,
                },
            )

        return result

    async def create_case_data(
        self,
        ctx: interactions.ModalContext,
        data: Dict[str, str],
        defendant_id: int,
    ) -> Data:
        sanitized_data = dict(
            filter(
                lambda x: x[1] is not None,
                ((k, self.sanitize_input(v)) for k, v in data.items()),
            )
        )

        current_time = datetime.now(timezone.utc).replace(microsecond=0)

        return Data(
            plaintiff_id=ctx.author.id,
            defendant_id=defendant_id,
            accusation=sanitized_data.get("accusation") or "",
            facts=sanitized_data.get("facts") or "",
            status=CaseStatus.FILED,
            judges=frozenset(),
            created_at=current_time,
            updated_at=current_time,
        ).__class__.model_validate(
            Data.model_dump(
                Data(
                    plaintiff_id=ctx.author.id,
                    defendant_id=defendant_id,
                    accusation=sanitized_data.get("accusation") or "",
                    facts=sanitized_data.get("facts") or "",
                    status=CaseStatus.FILED,
                    judges=frozenset(),
                    created_at=current_time,
                    updated_at=current_time,
                )
            )
        )

    @staticmethod
    async def create_new_chat_thread(
        channel: interactions.GuildText,
    ) -> interactions.ThreadChannel:
        thread_name: str = "聊天室"

        try:
            async with asyncio.timeout(5.0):
                thread: interactions.ThreadChannel = await channel.create_thread(
                    name=thread_name,
                    thread_type=interactions.ChannelType.GUILD_PUBLIC_THREAD,
                    auto_archive_duration=interactions.AutoArchiveDuration.ONE_WEEK,
                    reason="Chat room for lawsuit management",
                )
                logger.info(
                    "Created chat thread",
                    extra={"thread_id": thread.id, "thread_name": thread_name},
                )
                return thread

        except* HTTPException as eg:
            logger.error(
                "Failed to create chat thread",
                extra={"thread_name": thread_name, "error": str(eg)},
                exc_info=True,
            )
            raise

    async def create_chat_room_setup_embed(
        self,
        thread: interactions.ThreadChannel,
    ) -> interactions.Embed:
        try:
            async with asyncio.timeout(5.0), contextlib.AsyncExitStack():
                guild_id, thread_id = thread.guild.id, thread.id
                jump_url = f"discord://-/channels/{guild_id}/{thread_id}"
                # jump_url: str = f"https://discord.com/channels/{guild_id}/{thread_id}"

                message = (
                    f"The lawsuit and appeal buttons have been sent to this channel. "
                    f"The chat room thread is ready: [Jump to Thread]({jump_url})."
                )

                embed = await asyncio.shield(
                    self.view.generate_embed(
                        title="Success",
                        description=message,
                    )
                )

                logger.debug(
                    "Created chat room setup embed",
                    extra={
                        "thread_id": thread_id,
                        "jump_url": jump_url,
                        "timestamp": time.time_ns(),
                    },
                )
                return embed

        except* Exception as eg:
            logger.error(
                "Failed to create chat room setup embed",
                extra={
                    "thread_id": thread.id,
                    "error": str(eg),
                    "elapsed": time.monotonic(),
                },
                exc_info=True,
            )
            raise

    async def create_trial_thread(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        is_public: bool,
    ) -> None:
        thread_type = (
            interactions.ChannelType.GUILD_PUBLIC_THREAD
            if is_public
            else interactions.ChannelType.GUILD_PRIVATE_THREAD
        )
        thread_name = (
            f"Case #{case_id} {('Public' if is_public else 'Private')} Trial Chamber"
        )

        try:
            async with asyncio.timeout(10.0):
                async with asyncio.TaskGroup() as tg:
                    courtroom_task = tg.create_task(
                        self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                    )
                    roles_task = tg.create_task(self.get_allowed_members(ctx.guild))
                    courtroom_channel, allowed_roles = (
                        await courtroom_task,
                        await roles_task,
                    )

                    trial_thread = await courtroom_channel.create_thread(
                        name=thread_name,
                        thread_type=thread_type,
                        auto_archive_duration=interactions.AutoArchiveDuration.ONE_WEEK,
                        reason=f"Trial thread for case {case_id}",
                    )

                    case.trial_thread_id = trial_thread.id
                    case.allowed_roles = allowed_roles

                    async with asyncio.TaskGroup() as setup_tg:
                        embed_task = setup_tg.create_task(
                            self.view.create_summary_embed(case_id, case)
                        )
                        end_trial_button = self.view.create_end_trial_button(case_id)
                        embed = await embed_task

                        await trial_thread.send(
                            embeds=[embed],
                            components=[end_trial_button],
                        )

                        if is_public:
                            await self.delete_thread_created_message(
                                self.config.COURTROOM_CHANNEL_ID,
                                trial_thread,
                            )

                    visibility = "publicly" if is_public else "privately"
                    async with asyncio.TaskGroup() as notify_tg:
                        notify_tg.create_task(
                            self.view.send_success(
                                ctx,
                                f"Case #{case_id} will be tried {visibility}.",
                            )
                        )
                        notify_tg.create_task(
                            self.notify_case_participants(
                                case_id,
                                f"Case #{case_id} will be tried {visibility}.",
                            )
                        )
                        notify_tg.create_task(
                            self.add_members_to_thread(trial_thread, case.judges)
                        )

                logger.info(
                    "Created trial thread",
                    extra={
                        "case_id": case_id,
                        "thread_id": trial_thread.id,
                        "is_public": is_public,
                    },
                )

        except* HTTPException as eg:
            logger.error(
                "Failed to create trial thread",
                extra={
                    "case_id": case_id,
                    "is_public": is_public,
                    "error": str(eg),
                },
                exc_info=True,
            )
            await self.view.send_error(
                ctx,
                f"Failed to create trial thread for case {case_id}.",
            )
            raise

        except* Exception:
            logger.exception(
                "Unexpected error creating trial thread",
                extra={"case_id": case_id, "is_public": is_public},
            )
            await self.view.send_error(
                ctx,
                "An unexpected error occurred while setting up the trial thread.",
            )
            raise

    # Retrieval

    @staticmethod
    async def get_allowed_members(guild: interactions.Guild) -> frozenset[int]:
        case_role_names = {role.value for role in CaseRole}
        allowed_members: frozenset[int] = frozenset(
            member.id
            for member in guild.members
            if any(role.name in case_role_names for role in member.roles)
        )
        logger.debug(
            "Retrieved allowed members",
            extra={"member_count": len(allowed_members), "guild_id": guild.id},
        )
        return allowed_members

    async def get_judges(self, guild: interactions.Guild) -> frozenset[int]:
        judge_role = guild.get_role(self.config.JUDGE_ROLE_ID)
        if not judge_role:
            logger.warning(
                "Judge role not found",
                extra={"role_id": self.config.JUDGE_ROLE_ID, "guild_id": guild.id},
            )
            return frozenset()

        try:
            judges = frozenset(
                {
                    member.id
                    for member in judge_role.members
                    if any(r.id == self.config.JUDGE_ROLE_ID for r in member.roles)
                }
            )

            logger.debug(
                "Retrieved judges",
                extra={"judge_count": len(judges), "guild_id": guild.id},
            )
            return judges

        except Exception as e:
            logger.exception(
                "Failed to fetch judges", extra={"guild_id": guild.id, "error": str(e)}
            )
            return frozenset()

    async def courtroom_channel(self) -> interactions.GuildText:
        try:
            async with asyncio.timeout(3.0):
                channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                if not isinstance(channel, interactions.GuildText):
                    raise TypeError(f"Expected GuildText, got {type(channel).__name__}")
                logger.debug(
                    "Courtroom channel retrieved", extra={"channel_id": channel.id}
                )
                return channel
        except (asyncio.TimeoutError, HTTPException) as e:
            logger.error(
                "Failed to fetch courtroom channel",
                extra={"error": str(e), "channel_id": self.config.COURTROOM_CHANNEL_ID},
            )
            raise ValueError("Could not retrieve courtroom channel") from e

    async def get_or_create_chat_thread(
        self, channel: interactions.GuildText
    ) -> interactions.ThreadChannel:
        async with asyncio.timeout(5.0), contextlib.AsyncExitStack() as stack:
            try:
                return await asyncio.shield(
                    self.find_existing_chat_thread(channel)
                ) or await asyncio.create_task(
                    self.create_new_chat_thread(channel),
                    name=f"create_thread_{channel.id}",
                )
            except* Exception as eg:
                logger.error(
                    "Failed to get/create chat thread",
                    extra={
                        "channel_id": channel.id,
                        "error": str(eg),
                        "elapsed": time.monotonic()
                        - stack.enter_context(contextlib.nullcontext(time.monotonic())),
                    },
                )
                raise

    @staticmethod
    async def find_existing_chat_thread(
        channel: interactions.GuildText,
    ) -> Optional[interactions.ThreadChannel]:
        thread = None
        try:
            async with asyncio.timeout(2.5):
                active_threads = await asyncio.shield(channel.fetch_active_threads())
                thread = next(
                    itertools.compress(
                        active_threads.threads,
                        map(lambda t: t.name == "聊天室", active_threads.threads),
                    ),
                    None,
                )
                if thread is not None:
                    logger.debug(
                        "Found existing chat thread",
                        extra={"thread_id": thread.id},
                    )
        except* (asyncio.TimeoutError, HTTPException) as eg:
            logger.warning(
                "Failed to fetch active threads",
                extra={"error": str(eg), "channel_id": channel.id},
                exc_info=True,
            )
        return thread

    async def get_case(self, case_id: str) -> Optional[Data]:
        result = None
        try:
            async with asyncio.timeout(2.5):
                result = self.case_data_cache.get(
                    case_id
                ) or self.case_data_cache.setdefault(
                    case_id,
                    await asyncio.shield(
                        asyncio.create_task(
                            self.repo.get_case_data(case_id),
                            name=f"get_case_{case_id}",
                        )
                    ),
                )
                logger.debug(
                    "Case retrieved from %s",
                    "cache" if case_id in self.case_data_cache else "repository",
                    extra={"case_id": case_id},
                )
                result = self.case_data_cache[case_id]
        except* (asyncio.TimeoutError, Exception) as eg:
            logger.error(
                "Failed to get case",
                extra={"case_id": case_id, "error": str(eg)},
                exc_info=True,
            )
        return result

    async def get_user_active_case_number(self, user_id: int) -> Optional[str]:
        try:
            case_id = next(
                case_id
                for case_id, case_data in self.cached_cases.items()
                if user_id
                in {
                    (case := Data.model_validate(case_data)).plaintiff_id,
                    case.defendant_id,
                }
                and case.status != CaseStatus.CLOSED
            )
            logger.debug(
                "Found active case for user",
                extra={"user_id": user_id, "case_id": case_id},
            )
            return case_id
        except StopIteration:
            logger.debug("No active case found for user", extra={"user_id": user_id})
            return None

    def find_case_number_by_channel_id(self, channel_id: int) -> Optional[str]:
        try:
            return next(
                case_id
                for case_id, case_data in self.cached_cases.items()
                if channel_id
                in {
                    (case := Data.model_validate(case_data)).thread_id,
                    case.trial_thread_id,
                }
            )
        except StopIteration:
            logger.debug("No case found for channel", extra={"channel_id": channel_id})
            return None

    # Synchronization

    @contextlib.asynccontextmanager
    async def case_lock_manager(self, case_id: str) -> AsyncGenerator[None, None]:
        lock = await self.get_case_lock(case_id)
        start_time = time.perf_counter_ns()
        lock_acquired = False

        try:
            for attempt in (0, 1, 2):
                try:
                    timeout_ctx = asyncio.timeout(10.0)
                    async with timeout_ctx:
                        await lock.acquire()
                        lock_acquired = True
                        break
                except asyncio.TimeoutError:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1 << attempt)

            self.lock_timestamps[case_id] = asyncio.get_running_loop().time()

            self.lock_contention[case_id] = self.lock_contention.get(case_id, 0) + 1

            logger.debug(
                "Lock acquired",
                extra={
                    "case_id": case_id,
                    "contention_count": self.lock_contention[case_id],
                },
            )

            yield

        except asyncio.TimeoutError:
            logger.error(
                "Lock acquisition timeout after retries",
                extra={"case_id": case_id, "timeout": 10.0},
            )
            raise
        except Exception as e:
            logger.exception(
                "Lock acquisition failed", extra={"case_id": case_id, "error": str(e)}
            )
            raise
        finally:
            if lock_acquired and lock.locked():
                lock.release()
                duration = (time.perf_counter_ns() - start_time) * 1e-9
                logger.info(
                    "Lock released",
                    extra={
                        "case_id": case_id,
                        "duration_seconds": f"{duration:.9f}",
                        "contention_count": self.lock_contention[case_id],
                    },
                )

    async def get_case_lock(self, case_id: str) -> asyncio.Lock:
        try:
            return self.case_locks.setdefault(case_id, asyncio.Lock())
        except Exception as e:
            logger.exception(
                "Lock creation failed",
                extra={
                    "case_id": case_id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
                stack_info=True,
            )
            raise

    # Transcript

    async def enqueue_case_file_save(self, case_id: str, case: Data) -> None:
        async def save_case_file() -> None:
            try:
                channels = await asyncio.gather(
                    self.bot.fetch_channel(self.config.LOG_CHANNEL_ID),
                    self.bot.fetch_channel(self.config.LOG_FORUM_ID),
                )
                log_channel, log_forum = channels

                if not all(isinstance(c, interactions.GuildText) for c in channels):
                    raise RuntimeError("Failed to fetch required channels")

                log_post = await log_forum.fetch_message(self.config.LOG_POST_ID)
                file_name = f"case_{case_id}_{int(time.time())}.txt"
                content = await self.generate_case_transcript(case_id, case)

                temp_file = None
                try:
                    temp_file = await aiofiles.tempfile.NamedTemporaryFile(
                        mode="w+", delete=False, buffering=io.DEFAULT_BUFFER_SIZE
                    )

                    async with aiofiles.open(
                        temp_file.name,
                        mode="w",
                        encoding="utf-8",
                        buffering=io.DEFAULT_BUFFER_SIZE,
                    ) as async_file:
                        await async_file.write(content)
                        await async_file.flush()
                        os.fsync(async_file.fileno())

                        file_obj = interactions.File(temp_file.name, file_name)
                        message = f"Case {case_id} details saved at {datetime.now(timezone.utc).isoformat()}"

                        async with asyncio.timeout(10):
                            await asyncio.gather(
                                log_channel.send(message, files=[file_obj]),
                                log_post.reply(message, files={"files": [file_obj]}),
                            )
                finally:
                    if temp_file:
                        await aiofiles.os.remove(temp_file.name)

            except (asyncio.TimeoutError, HTTPException) as e:
                logger.error(
                    f"Communication error saving case file {case_id}", exc_info=e
                )
                raise
            except Exception:
                logger.exception(f"Critical error saving case file for case {case_id}")
                raise

        try:
            backoff = 0.5
            for attempt in range(3):
                try:
                    async with asyncio.timeout(5):
                        await self.case_task_queue.put(save_case_file)
                        return
                except asyncio.TimeoutError:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(backoff)
                    backoff *= 2
        except Exception:
            logger.exception(f"Failed to enqueue case file save for case {case_id}")
            raise

    async def generate_case_transcript(self, case_id: str, case: Data) -> str:
        now = datetime.now(timezone.utc)
        header = await self.generate_report_header(case_id, case, now)

        async def fetch_channel_transcript(channel_id: Optional[int]) -> Optional[str]:
            return (
                None
                if not channel_id
                else await self.generate_thread_transcript(
                    await self.bot.fetch_channel(channel_id)
                )
            )

        async with asyncio.TaskGroup() as tg:
            transcripts = [
                t
                for t in (
                    await tg.create_task(fetch_channel_transcript(case.thread_id)),
                    await tg.create_task(
                        fetch_channel_transcript(case.trial_thread_id)
                    ),
                )
                if t
            ]

        return f"{header}{chr(10).join(transcripts)}"

    async def generate_thread_transcript(
        self, thread: Optional[interactions.GuildText]
    ) -> Optional[str]:
        if thread is None:
            return None

        try:
            async with asyncio.timeout(30):
                messages = tuple(reversed(await thread.history(limit=0).flatten()))
                formatted_messages = [None] * len(messages)

                async with asyncio.TaskGroup() as tg:
                    tasks = {
                        idx: tg.create_task(self.format_message_for_transcript(msg))
                        for idx, msg in enumerate(messages)
                    }
                    for idx, task in tasks.items():
                        formatted_messages[idx] = await task

                return f"\n## {thread.name}\n\n" + "\n".join(
                    filter(None, formatted_messages)
                )

        except (asyncio.TimeoutError, Exception) as e:
            logger.exception(
                "Thread transcript generation failed",
                extra={"thread_id": thread.id, "error": str(e)},
            )
            return None

    @staticmethod
    async def generate_report_header(case_id: str, case: Data, now: datetime) -> str:
        return (
            "".join(
                f"- **{n}:** {v}\n"
                for n, v in (
                    ("Case ID", case_id),
                    ("Plaintiff ID", str(case.plaintiff_id)),
                    ("Defendant ID", str(case.defendant_id)),
                    ("Accusation", case.accusation or ""),
                    ("Facts", case.facts or ""),
                    ("Filing Time", now.isoformat()),
                    ("Case Status", getattr(case.status, "name", str(case.status))),
                )
            )
            + "\n"
        )

    @staticmethod
    async def format_message_for_transcript(message: interactions.Message) -> str:
        return "".join(
            (
                *[
                    f"{message.author} at {message.created_at.isoformat()}: {message.content}\n"
                ],
                *(
                    f"Attachments: {','.join(a.url for a in message.attachments)}\n"
                    if message.attachments
                    else []
                ),
                *(
                    f"Edited at {message.edited_timestamp.isoformat()}\n"
                    if message.edited_timestamp is not None
                    else []
                ),
            )
        )
