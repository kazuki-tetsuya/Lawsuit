from __future__ import annotations

import asyncio
import copy
import functools
import io
import logging
import logging.handlers
import operator
import os
import random
import re
import secrets
import tracemalloc
from asyncio import TimeoutError
from collections import ChainMap, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum, IntEnum
from functools import cached_property, lru_cache, partial, wraps
from logging.handlers import RotatingFileHandler
from time import perf_counter
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    Final,
    FrozenSet,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    ParamSpec,
    Protocol,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    final,
    runtime_checkable,
)

import aiohttp
import bleach
import interactions
import lmdb
import orjson
from asyncache import cached
from cachetools import TTLCache
from cytoolz import curry
from interactions.api.events import ExtensionUnload, MessageCreate, NewThreadCreate
from interactions.client.errors import Forbidden, HTTPException, NotFound
from pydantic import BaseModel, Field, ValidationError, root_validator, validator
from pydantic.fields import PrivateAttr

LOG_DIR: Final[str] = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE: Final[str] = os.path.join(LOG_DIR, "lawsuit.log")
logger: Final[logging.Logger] = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler: Final[RotatingFileHandler] = RotatingFileHandler(
    LOG_FILE, maxBytes=1 * 1024 * 1024, backupCount=1
)
file_handler.setLevel(logging.DEBUG)
formatter: Final[logging.Formatter] = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


T = TypeVar("T")
P = ParamSpec("P")

# Model


@dataclass(frozen=True, slots=True)
class Config:
    GUILD_ID: Final[int] = field(default=1150630510696075404)
    JUDGE_ROLE_ID: Final[int] = field(default=1200100104682614884)
    PLAINTIFF_ROLE_ID: Final[int] = field(default=1200043628899356702)
    COURTROOM_CHANNEL_ID: Final[int] = field(default=1247290032881008701)
    MAX_JUDGES_PER_CASE: Final[int] = field(default=1)
    MAX_JUDGES_PER_APPEAL: Final[int] = field(default=3)
    MAX_FILE_SIZE: Final[int] = field(default=10 * 1024 * 1024)
    ALLOWED_MIME_TYPES: Final[FrozenSet[str]] = field(
        default_factory=lambda: frozenset(
            {"image/jpeg", "image/png", "application/pdf", "text/plain"}
        )
    )
    _instance: ClassVar[Optional[Config]] = None

    def __post_init__(self) -> None:
        if self.__class__._instance is not None:
            raise RuntimeError(
                "Config instance already exists. Use get_instance() to access it."
            )
        object.__setattr__(self.__class__, "_instance", self)

    @classmethod
    @lru_cache(maxsize=None)
    def get_instance(cls: Type[T]) -> T:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __getattribute__(self, name: str) -> Any:
        value = super().__getattribute__(name)
        return (
            value
            if name.startswith("_") or isinstance(value, (int, str, tuple, frozenset))
            else copy.deepcopy(value)
        )


@final
class CaseStatus(Enum):
    FILED = "FILED"
    IN_PROGRESS = "IN_PROGRESS"
    CLOSED = "CLOSED"


@final
class EmbedColor(IntEnum):
    OFF = 0x5D5A58
    FATAL = 0xFF4343
    ERROR = 0xE81123
    WARN = 0xFFB900
    INFO = 0x0078D7
    DEBUG = 0x00B7C3
    TRACE = 0x8E8CD8
    ALL = 0x0063B1


@final
class CaseRole(Enum):
    PROSECUTOR = "公诉人"
    PRIVATE_PROSECUTOR = "自诉人"
    PLAINTIFF = "原告人"
    AGENT = "代理人"
    DEFENDANT = "被告人"
    DEFENDER = "辩护人"
    WITNESS = "证人"


@final
class UserAction(Enum):
    MUTE = "MUTE"
    UNMUTE = "UNMUTE"
    PIN = "PIN"
    DELETE = "DELETE"
    ASSIGN = "ASSIGN"
    REVOKE = "REVOKE"


@final
class EvidenceAction(Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@final
class CaseAction(Enum):
    FILE = "file"
    CLOSE = "close"
    WITHDRAW = "withdraw"
    ACCEPT = "accept"
    DISMISS = "dismiss"

    @cached_property
    def display_name(self) -> str:
        return {
            CaseAction.FILE: "立案",
            CaseAction.CLOSE: "结案",
            CaseAction.WITHDRAW: "撤诉",
            CaseAction.ACCEPT: "受理",
            CaseAction.DISMISS: "驳回",
        }[self]

    @cached_property
    def new_status(self) -> CaseStatus:
        return {
            CaseAction.WITHDRAW: CaseStatus.CLOSED,
            CaseAction.CLOSE: CaseStatus.CLOSED,
            CaseAction.ACCEPT: CaseStatus.IN_PROGRESS,
            CaseAction.DISMISS: CaseStatus.CLOSED,
            CaseAction.FILE: CaseStatus.FILED,
        }[self]


class Evidence(BaseModel):
    user_id: int = Field(..., gt=0, description="User ID of the evidence submitter")
    content: str = Field(
        ..., min_length=1, max_length=4000, description="Content of the evidence"
    )
    attachments: Tuple[Dict[str, str], ...] = Field(
        default_factory=tuple, description="Tuple of attachment metadata"
    )
    message_id: int = Field(
        ..., gt=0, description="Discord message ID of the evidence submission"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp of evidence submission",
    )
    state: EvidenceAction = Field(
        default=EvidenceAction.PENDING, description="Current state of the evidence"
    )

    class Config:
        frozen = True
        allow_mutation = False

    @validator("state")
    def validate_state(cls, v: EvidenceAction) -> EvidenceAction:
        if v not in EvidenceAction.__members__.values():
            raise ValueError(f"Invalid state: {v}")
        return v

    @validator("attachments", pre=True)
    def validate_attachments(cls, v: Any) -> Tuple[Dict[str, str], ...]:
        if isinstance(v, list):
            return tuple(v)
        if isinstance(v, tuple):
            return v
        raise ValueError("Attachments must be a list or tuple")


class Data(BaseModel):
    plaintiff_id: int = Field(..., gt=0, description="ID of the plaintiff")
    defendant_id: int = Field(..., gt=0, description="ID of the defendant")
    status: CaseStatus = Field(
        default=CaseStatus.FILED, description="Current status of the case"
    )
    thread_id: Optional[int] = Field(
        None, gt=0, description="Thread ID associated with the case"
    )
    judges: FrozenSet[int] = Field(
        default_factory=frozenset, description="Set of judge IDs"
    )
    trial_thread_id: Optional[int] = Field(None, gt=0, description="Trial thread ID")
    allowed_roles: FrozenSet[int] = Field(
        default_factory=frozenset, description="Roles allowed in the trial thread"
    )
    log_message_id: Optional[int] = Field(None, gt=0, description="Log message ID")
    mute_list: FrozenSet[int] = Field(
        default_factory=frozenset, description="Set of user IDs who are muted"
    )
    roles: Dict[str, FrozenSet[int]] = Field(
        default_factory=dict, description="Roles assigned within the case"
    )
    evidence_queue: Tuple[Evidence, ...] = Field(
        default_factory=tuple, description="Queue of evidence submissions"
    )
    accusation: Optional[str] = Field(
        None, max_length=1000, description="Accusation details"
    )
    facts: Optional[str] = Field(None, max_length=2000, description="Facts of the case")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp of case creation",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp of last update",
    )

    WHITESPACE_REGEX: Final[re.Pattern] = re.compile(r"\s+")
    _hash: int = PrivateAttr()

    class Config:
        validate_assignment = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            CaseStatus: lambda v: v.value,
            FrozenSet: lambda v: list(v),
        }
        frozen = True
        allow_mutation = False

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        object.__setattr__(self, "_hash", hash(frozenset(self.__dict__.items())))

    def __hash__(self) -> int:
        return self._hash

    @validator(
        "plaintiff_id",
        "defendant_id",
        "thread_id",
        "trial_thread_id",
        "log_message_id",
        pre=True,
    )
    def validate_positive_int(cls, v: Any) -> Optional[int]:
        if v is None:
            return v
        try:
            v = int(v)
        except ValueError:
            raise ValueError("Must be a valid integer")
        if v <= 0:
            raise ValueError("Must be a positive integer")
        return v

    @validator("accusation", "facts")
    def validate_text_fields(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            return cls.WHITESPACE_REGEX.sub(" ", v) if v else None
        return None

    @validator("mute_list", "judges", "allowed_roles", pre=True)
    def validate_id_sets(cls, v: Any) -> FrozenSet[int]:
        if isinstance(v, str):
            return frozenset(int(id.strip()) for id in v.split(",") if id.strip())
        return frozenset(v) if isinstance(v, (list, set, frozenset)) else frozenset()

    @validator("roles", pre=True)
    def validate_roles(cls, v: Dict[str, Any]) -> Dict[str, FrozenSet[int]]:
        return {
            k: frozenset(v) if isinstance(v, (list, set)) else v for k, v in v.items()
        }

    @validator("evidence_queue", pre=True)
    def validate_evidence_queue(cls, v: Any) -> Tuple[Evidence, ...]:
        return tuple(Evidence(**item) if isinstance(item, dict) else item for item in v)

    @root_validator
    def check_thread_ids(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        thread_id, trial_thread_id = values.get("thread_id"), values.get(
            "trial_thread_id"
        )
        if (
            thread_id is not None
            and trial_thread_id is not None
            and thread_id == trial_thread_id
        ):
            raise ValueError("thread_id and trial_thread_id must be different")
        return values

    @root_validator
    def check_plaintiff_defendant(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        plaintiff_id, defendant_id = values.get("plaintiff_id"), values.get(
            "defendant_id"
        )
        if (
            plaintiff_id is not None
            and defendant_id is not None
            and plaintiff_id == defendant_id
        ):
            raise ValueError("plaintiff_id and defendant_id must be different")
        return values

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        return cast(T, cls(**data))

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        d = super().dict(*args, **kwargs)
        d["judges"] = list(d["judges"])
        d["allowed_roles"] = list(d["allowed_roles"])
        d["mute_list"] = list(d["mute_list"])
        d["roles"] = {k: list(v) for k, v in d["roles"].items()}
        d["evidence_queue"] = list(d["evidence_queue"])
        return d


@runtime_checkable
class LMDB(Protocol):
    def begin(self, write: bool = ..., buffers: bool = ...) -> Any: ...
    def close(self) -> None: ...


class Store:
    def __init__(self: T, config: Config) -> None:
        self.config: Final[Config] = config
        self.env: Optional[LMDB] = None
        self.locks: Dict[str, asyncio.Lock] = {}
        self.global_lock: asyncio.Lock = asyncio.Lock()
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=4)
        self._open_db()

    def _open_db(self: T) -> None:
        try:
            self.env = cast(
                LMDB,
                lmdb.open(
                    "cases.lmdb",
                    map_size=1 * 1024 * 1024,
                    max_dbs=1,
                    subdir=True,
                    readonly=False,
                    lock=True,
                    readahead=False,
                    meminit=False,
                    max_readers=126,
                    sync=True,
                    map_async=True,
                    mode=0o600,
                ),
            )
        except lmdb.Error as e:
            raise RuntimeError(f"Failed to open database: {e}") from e

    def close_db(self: T) -> None:
        if self.env:
            self.env.close()
            self.env = None
        self.thread_pool.shutdown(wait=True)

    @asynccontextmanager
    async def db_operation(self: T) -> AsyncGenerator[LMDB, None]:
        async with self.global_lock:
            if self.env is None:
                self._open_db()
            try:
                yield cast(LMDB, self.env)
            except lmdb.Error as e:
                raise RuntimeError(f"Database operation failed: {e}") from e

    async def execute_db_operation(
        self: T,
        operation: Callable[[LMDB, Optional[str], Optional[Dict[str, Any]]], Any],
        case_id: Optional[str] = None,
        case_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        async with self.db_operation() as env:
            if case_id:
                async with self.locks.setdefault(case_id, asyncio.Lock()):
                    return await asyncio.get_running_loop().run_in_executor(
                        self.thread_pool, operation, env, case_id, case_data
                    )
            else:
                return await asyncio.get_running_loop().run_in_executor(
                    self.thread_pool, operation, env, None, None
                )

    async def upsert_case(self: T, case_id: str, case_data: Dict[str, Any]) -> None:
        await self.execute_db_operation(self._upsert_case_sync, case_id, case_data)

    async def delete_case(self: T, case_id: str) -> None:
        await self.execute_db_operation(self._delete_case_sync, case_id)

    async def get_all_cases(self: T) -> Dict[str, Dict[str, Any]]:
        return await self.execute_db_operation(self._get_all_cases_sync)

    async def get_case(self: T, case_id: str) -> Optional[Dict[str, Any]]:
        return await self.execute_db_operation(self._get_case_sync, case_id)

    @staticmethod
    def _upsert_case_sync(
        env: LMDB, case_id: str, case_data: Optional[Dict[str, Any]]
    ) -> None:
        if case_data is None:
            raise ValueError("case_data cannot be None for upsert operation")
        with env.begin(write=True) as txn:
            txn.put(case_id.encode("utf-8"), orjson.dumps(case_data))

    @staticmethod
    def _delete_case_sync(env: LMDB, case_id: str, _: Optional[Dict[str, Any]]) -> None:
        with env.begin(write=True) as txn:
            txn.delete(case_id.encode("utf-8"))

    @staticmethod
    def _get_all_cases_sync(
        env: LMDB, _: Optional[str], __: Optional[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        with env.begin(buffers=True) as txn:
            return {
                key.decode("utf-8"): orjson.loads(value) for key, value in txn.cursor()
            }

    @staticmethod
    def _get_case_sync(
        env: LMDB, case_id: Optional[str], _: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if case_id is None:
            raise ValueError("case_id cannot be None for get_case operation")
        with env.begin(buffers=True) as txn:
            case_data = txn.get(case_id.encode("utf-8"))
            return orjson.loads(case_data) if case_data else None


def async_retry(max_retries: int = 3, delay: float = 0.1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Retrying {func.__name__} due to {str(e)}")
                    await asyncio.sleep(delay * (2**attempt))

        return wrapper

    return decorator


class Repository(Generic[T]):
    def __init__(self, config: Config, case_store: Store):
        self.config: Final[Config] = config
        self.case_store: Final[Store] = case_store
        self.current_cases: TTLCache[str, T] = TTLCache(maxsize=1000, ttl=3600)
        self._case_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._global_lock: asyncio.Lock = asyncio.Lock()

    @asynccontextmanager
    async def case_lock(self, case_id: str) -> AsyncIterator[None]:
        async with self._global_lock:
            lock = self._case_locks[case_id]
        try:
            await asyncio.wait_for(lock.acquire(), timeout=5.0)
            yield
        finally:
            lock.release()

    @async_retry()
    async def get_case(self, case_id: str) -> Optional[T]:
        if case := self.current_cases.get(case_id):
            return case

        case_data = await self.case_store.get_case(case_id)
        if case_data is None:
            return None

        try:
            case = self._create_case_instance(case_data)
            self.current_cases[case_id] = case
            return case
        except ValidationError as e:
            logger.error(f"Error retrieving case {case_id}: {str(e)}")
            return None

    @async_retry()
    async def persist_case(self, case_id: str, case: T) -> None:
        async with self.case_lock(case_id):
            self.current_cases[case_id] = case
            await self.case_store.upsert_case(case_id, case.dict())

    @async_retry()
    async def update_case(self, case_id: str, **kwargs: Any) -> None:
        async with self.case_lock(case_id):
            if case := await self.get_case(case_id):
                updated_case = self._update_case_instance(case, **kwargs)
                await self.persist_case(case_id, updated_case)
            else:
                logger.error(f"Case {case_id} not found for update")

    @async_retry()
    async def delete_case(self, case_id: str) -> None:
        async with self.case_lock(case_id):
            await self.case_store.delete_case(case_id)
            self.current_cases.pop(case_id, None)
            async with self._global_lock:
                self._case_locks.pop(case_id, None)

    @async_retry()
    async def get_all_cases(self) -> Dict[str, T]:
        case_data = await self.case_store.get_all_cases()
        return {
            case_id: self._create_case_instance(data)
            for case_id, data in case_data.items()
            if self._validate_case_data(case_id, data)
        }

    def _validate_case_data(self, case_id: str, data: Dict[str, Any]) -> bool:
        try:
            self._create_case_instance(data)
            return True
        except ValidationError as e:
            logger.error(f"Invalid case data for case {case_id}: {str(e)}")
            return False

    @asynccontextmanager
    async def transaction(self):
        async with self.case_store.db_operation() as db:
            yield db

    @staticmethod
    @curry
    def _create_case_instance(data: Dict[str, Any]) -> T:
        return Data(**data)

    @staticmethod
    @curry
    def _update_case_instance(case: T, **kwargs: Any) -> T:
        return case.copy(update=kwargs)

    async def get_cases_by_status(self, status: CaseStatus) -> Dict[str, T]:
        all_cases = await self.get_all_cases()
        return {
            case_id: case
            for case_id, case in all_cases.items()
            if case.status == status
        }

    async def get_cases_by_user(self, user_id: int) -> Dict[str, T]:
        all_cases = await self.get_all_cases()
        return {
            case_id: case
            for case_id, case in all_cases.items()
            if case.plaintiff_id == user_id or case.defendant_id == user_id
        }

    async def cleanup_expired_cases(self, expiration_days: int) -> None:
        current_time = datetime.now(timezone.utc)
        expiration_threshold = current_time - timedelta(days=expiration_days)

        all_cases = await self.get_all_cases()
        expired_cases = [
            case_id
            for case_id, case in all_cases.items()
            if case.status == CaseStatus.CLOSED
            and case.updated_at < expiration_threshold
        ]

        for case_id in expired_cases:
            await self.delete_case(case_id)

    @asynccontextmanager
    async def bulk_operation(self):
        async with self._global_lock:
            yield self

    async def __aenter__(self):
        await self._global_lock.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._global_lock.release()


# View


class View:
    def __init__(self, bot: interactions.Client, config: Config):
        self.bot: Final[interactions.Client] = bot
        self.config: Final[Config] = config
        self.embed_cache: Dict[str, interactions.Embed] = {}
        self.button_cache: Dict[str, List[interactions.Button]] = {}

    async def create_embed(
        self, title: str, description: str = "", color: EmbedColor = EmbedColor.INFO
    ) -> interactions.Embed:
        cache_key = f"{title}:{description}:{color.value}"
        if cache_key in self.embed_cache:
            return self.embed_cache[cache_key]

        embed = interactions.Embed(
            title=title, description=description, color=color.value
        )
        guild: Optional[interactions.Guild] = await self.bot.fetch_guild(
            self.config.GUILD_ID
        )
        if guild and guild.icon:
            embed.set_footer(text=guild.name, icon_url=guild.icon.url)
        embed.timestamp = datetime.now()
        embed.set_footer(text="鍵政大舞台")

        self.embed_cache[cache_key] = embed
        return embed

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

    @lru_cache(maxsize=2)
    def create_lawsuit_modal(self, is_appeal: bool = False) -> interactions.Modal:
        title = "Appeal Form" if is_appeal else "Lawsuit Form"
        custom_id = f"{'appeal' if is_appeal else 'lawsuit'}_form_modal"
        fields = self.define_modal_fields(is_appeal)
        return interactions.Modal(*fields, title=title, custom_id=custom_id)

    @staticmethod
    @lru_cache(maxsize=2)
    def define_modal_fields(
        is_appeal: bool,
    ) -> Tuple[interactions.ShortText | interactions.ParagraphText, ...]:
        fields = (
            (
                "defendant_id",
                "Case Number" if is_appeal else "Defendant ID",
                (
                    "Enter the original case number"
                    if is_appeal
                    else "Enter the defendant's user ID"
                ),
            ),
            (
                "accusation",
                "Reason for Appeal" if is_appeal else "Accusation",
                "Enter the reason for appeal" if is_appeal else "刑法条文",
            ),
            ("facts", "事实", "Describe the relevant facts"),
        )
        return tuple(
            (
                interactions.ShortText(
                    custom_id=field[0],
                    label=field[1],
                    placeholder=field[2],
                    required=True,
                )
                if field[0] != "facts"
                else interactions.ParagraphText(
                    custom_id=field[0],
                    label=field[1],
                    placeholder=field[2],
                    required=True,
                )
            )
            for field in fields
        )

    @lru_cache(maxsize=1)
    def create_action_buttons(self) -> Tuple[interactions.Button, ...]:
        return (
            interactions.Button(
                style=interactions.ButtonStyle.PRIMARY,
                label="提起诉讼",
                custom_id="initiate_lawsuit_button",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.SECONDARY,
                label="提起上诉",
                custom_id="initiate_appeal_button",
            ),
        )

    async def create_case_summary_embed(
        self, case_id: str, case: Data
    ) -> interactions.Embed:
        fields = (
            ("主审法官", " ".join(f"<@{judge_id}>" for judge_id in case.judges)),
            ("原告", f"<@{case.plaintiff_id}>"),
            ("被告", f"<@{case.defendant_id}>"),
            ("指控", case.accusation),
            ("事实", case.facts),
        )

        embed = await self.create_embed(f"第{case_id}号案")
        for name, value in fields:
            embed.add_field(name=name, value=value, inline=False)

        return embed

    def create_case_action_buttons(
        self, case_id: str
    ) -> Tuple[interactions.Button, ...]:
        cache_key = f"case_action_{case_id}"
        if cache_key in self.button_cache:
            return tuple(self.button_cache[cache_key])

        buttons = (
            interactions.Button(
                style=interactions.ButtonStyle.DANGER,
                label="驳回",
                custom_id=f"dismiss_{case_id}",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.SUCCESS,
                label="立案",
                custom_id=f"accept_{case_id}",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.SECONDARY,
                label="撤诉",
                custom_id=f"withdraw_{case_id}",
            ),
        )
        self.button_cache[cache_key] = buttons
        return buttons

    def create_trial_privacy_buttons(
        self, case_id: str
    ) -> Tuple[interactions.Button, ...]:
        cache_key = f"trial_privacy_{case_id}"
        if cache_key in self.button_cache:
            return tuple(self.button_cache[cache_key])

        buttons = (
            interactions.Button(
                style=interactions.ButtonStyle.SUCCESS,
                label="是",
                custom_id=f"public_trial_yes_{case_id}",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.DANGER,
                label="否",
                custom_id=f"public_trial_no_{case_id}",
            ),
        )
        self.button_cache[cache_key] = buttons
        return buttons

    @lru_cache(maxsize=1000)
    def create_end_trial_button(self, case_id: str) -> interactions.Button:
        return interactions.Button(
            style=interactions.ButtonStyle.DANGER,
            label="结案",
            custom_id=f"end_trial_{case_id}",
        )

    @lru_cache(maxsize=1000)
    def create_user_management_buttons(
        self, user_id: int
    ) -> Tuple[interactions.Button, ...]:
        actions = (
            ("禁言", "mute", interactions.ButtonStyle.PRIMARY),
            ("解禁", "unmute", interactions.ButtonStyle.SUCCESS),
        )
        return tuple(
            interactions.Button(
                style=style, label=label, custom_id=f"{action}_{user_id}"
            )
            for label, action, style in actions
        )

    @lru_cache(maxsize=1000)
    def create_message_management_buttons(
        self, message_id: int
    ) -> Tuple[interactions.Button, ...]:
        return (
            interactions.Button(
                style=interactions.ButtonStyle.PRIMARY,
                label="标记",
                custom_id=f"pin_{message_id}",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.DANGER,
                label="删除",
                custom_id=f"delete_{message_id}",
            ),
        )

    @lru_cache(maxsize=1000)
    def create_evidence_action_buttons(
        self, case_id: str, user_id: int
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

    PROVERBS: Final[Tuple[str, ...]] = (
        "Iuris prudentia est divinarum atque humanarum rerum notitia, iusti atque iniusti scientia (D. 1, 1, 10, 2).",
        "Iuri operam daturum prius nosse oportet, unde nomen iuris descendat. est autem a iustitia appellatum: nam, ut eleganter celsus definit, ius est ars boni et aequi (D. 1, 1, 1, pr.).",
        "Iustitia est constans et perpetua voluntas ius suum cuique tribuendi (D. 1, 1, 10, pr.).",
    )

    @staticmethod
    @lru_cache(maxsize=1)
    def get_proverb_selector() -> Callable[[date], str]:
        proverb_count = len(View.PROVERBS)
        random.seed(0)
        shuffled_indices = tuple(range(proverb_count))
        random.shuffle(shuffled_indices)

        def select_proverb(current_date: date) -> str:
            index = shuffled_indices[current_date.toordinal() % proverb_count]
            return View.PROVERBS[index]

        return select_proverb

    @lru_cache(maxsize=366)
    def get_daily_proverb(self, current_date: date = date.today()) -> str:
        return self.get_proverb_selector()(current_date)

    def clear_caches(self) -> None:
        self.embed_cache.clear()
        self.button_cache.clear()
        for cached_func in (
            self.create_lawsuit_modal,
            self.define_modal_fields,
            self.create_action_buttons,
            self.create_end_trial_button,
            self.create_user_management_buttons,
            self.create_message_management_buttons,
            self.create_evidence_action_buttons,
            self.get_proverb_selector,
            self.get_daily_proverb,
        ):
            cached_func.cache_clear()

    async def update_embed_cache(self) -> None:
        tasks = [
            self.create_embed(title, description, color)
            for title, description, color in self.embed_cache.keys()
        ]
        updated_embeds = await asyncio.gather(*tasks)
        self.embed_cache = dict(
            ChainMap(
                {k: v for k, v in zip(self.embed_cache.keys(), updated_embeds)},
                self.embed_cache,
            )
        )


# Controller


class Lawsuit(interactions.Extension):
    def __init__(self, bot: interactions.Client):
        self.bot: Final[interactions.Client] = bot
        self.config: Final[Config] = Config.get_instance()
        self.store: Final[Store] = Store(self.config)
        self.repository: Final[Repository] = Repository(self.config, self.store)
        self.view: Final[View] = View(bot, self.config)
        self.current_cases: Final[Dict[str, Data]] = {}
        self.case_cache: TTLCache[str, Data] = TTLCache(maxsize=1000, ttl=300)
        self._case_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._lock_timestamps: Dict[str, float] = {}
        self._case_lock: Final[asyncio.Lock] = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self.case_task_queue: Final[asyncio.Queue[Callable[[], Awaitable[None]]]] = (
            asyncio.Queue()
        )
        self.shutdown_event: Final[asyncio.Event] = asyncio.Event()
        self.lawsuit_button_message_id: Optional[int] = None
        self.case_action_handlers: Final[Dict[CaseAction, Callable]] = {
            CaseAction.FILE: self.handle_file_case_request,
            CaseAction.WITHDRAW: partial(
                self.handle_case_closure, action=CaseAction.WITHDRAW
            ),
            CaseAction.CLOSE: partial(
                self.handle_case_closure, action=CaseAction.CLOSE
            ),
            CaseAction.DISMISS: partial(
                self.handle_case_closure, action=CaseAction.DISMISS
            ),
            CaseAction.ACCEPT: self.handle_accept_case_request,
        }
        self.case_lock_cache: TTLCache = TTLCache(maxsize=1000, ttl=300)
        self._initialize_background_tasks()

    # Component functions

    @interactions.listen(NewThreadCreate)
    async def on_thread_create(self, event: NewThreadCreate) -> None:
        if event.thread.parent_id == self.config.COURTROOM_CHANNEL_ID:
            await self.delete_thread_created_message(
                event.thread.parent_id, event.thread
            )

    @interactions.listen(MessageCreate)
    async def on_message_create(self, message: interactions.Message) -> None:
        if message.author.id == self.bot.user.id:
            return

        coro = (
            self.handle_direct_message_evidence(message)
            if isinstance(message.channel, interactions.DMChannel)
            else self.handle_channel_message(message)
        )
        asyncio.create_task(coro)

    @interactions.listen()
    async def on_component(self, component: interactions.ComponentContext) -> None:
        custom_id = component.custom_id
        action, *args = custom_id.partition("_")[::2]

        handler = self.ACTION_MAP.get(action)
        if not handler:
            logger.warning(f"Unhandled component interaction: {custom_id}")
            return

        try:
            if action in self.CASE_ACTIONS:
                await handler(self, component, CaseAction[action.upper()], args[0])
            else:
                await handler(self, component, *args)
        except Exception as e:
            logger.error(
                f"Error handling component {custom_id}: {str(e)}", exc_info=True
            )
            await component.send(
                "An error occurred while processing your request.", ephemeral=True
            )

    ACTION_MAP: Final[Dict[str, Callable]] = {
        "file": lambda self, ctx, action, case_id: self.handle_case_action(
            ctx, action, case_id
        ),
        "close": lambda self, ctx, action, case_id: self.handle_case_action(
            ctx, action, case_id
        ),
        "withdraw": lambda self, ctx, action, case_id: self.handle_case_action(
            ctx, action, case_id
        ),
        "accept": lambda self, ctx, action, case_id: self.handle_case_action(
            ctx, action, case_id
        ),
        "dismiss": lambda self, ctx, action, case_id: self.handle_case_action(
            ctx, action, case_id
        ),
        "public_trial": lambda self, ctx, *args: self.handle_trial_privacy(ctx),
        "end_trial": lambda self, ctx, *args: self.handle_end_trial(ctx),
        "mute": lambda self, ctx, *args: self.handle_user_message_action(ctx),
        "unmute": lambda self, ctx, *args: self.handle_user_message_action(ctx),
        "pin": lambda self, ctx, *args: self.handle_user_message_action(ctx),
        "delete": lambda self, ctx, *args: self.handle_user_message_action(ctx),
        "approve_evidence": lambda self, ctx, *args: self.handle_judge_evidence_action(
            ctx
        ),
        "reject_evidence": lambda self, ctx, *args: self.handle_judge_evidence_action(
            ctx
        ),
    }

    CASE_ACTIONS: Final[frozenset] = frozenset(
        {"file", "close", "withdraw", "accept", "dismiss"}
    )

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self) -> None:
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._cleanup_task), timeout=5.0)
            except asyncio.TimeoutError:
                logger.error("Cleanup task timed out during extension unload")

    @interactions.component_callback("initiate_lawsuit_button")
    async def handle_lawsuit_initiation(
        self, ctx: interactions.ComponentContext
    ) -> None:
        if await self.has_ongoing_lawsuit(ctx.author.id):
            await self.view.send_error(ctx, "You already have an ongoing lawsuit.")
        else:
            await ctx.send_modal(self.view.create_lawsuit_modal())

    @interactions.modal_callback("lawsuit_form_modal")
    async def handle_lawsuit_form(self, ctx: interactions.ModalContext) -> None:
        try:
            if not (is_valid := self.validate_and_sanitize(ctx.responses))[0]:
                await self.view.send_error(
                    ctx, "Invalid input. Please check your entries and try again."
                )
                return

            sanitized_data = is_valid[1]
            if not (
                is_valid_defendant := await self.validate_defendant(sanitized_data)
            )[0]:
                await self.view.send_error(
                    ctx, "Invalid defendant ID. Please check and try again."
                )
                return

            defendant_id = is_valid_defendant[1]
            if not (
                case_data := await self.create_new_case(
                    ctx, sanitized_data, defendant_id
                )
            ):
                await self.view.send_error(
                    ctx,
                    "An error occurred while creating the case. Please try again later.",
                )
                return

            case_id, case = case_data
            if not await self.setup_mediation_thread(case_id, case):
                await self.repository.delete_case(case_id)
                await self.view.send_error(
                    ctx,
                    "An error occurred while setting up the mediation thread. Please try again later.",
                )
                return

            await asyncio.gather(
                self.view.send_success(ctx, f"第{case_id}号调解室创建成功"),
                self.notify_judges(case_id, ctx.guild_id),
            )
        except Exception as e:
            logger.error(
                f"Unhandled exception in handle_lawsuit_form: {str(e)}", exc_info=True
            )
            await self.view.send_error(
                ctx, "An unexpected error occurred. Please try again later."
            )

    # Task functions

    async def cleanup_locks(self) -> None:
        async with self._case_lock:
            self._case_locks.clear()
            self._lock_timestamps.clear()

    def __del__(self) -> None:
        if self._cleanup_task and not self._cleanup_task.done():
            asyncio.get_event_loop().create_task(self._cleanup_task)

    def _initialize_background_tasks(self) -> None:
        loop = asyncio.get_running_loop()
        tasks = [
            self.fetch_cases(),
            self.daily_embed_update_task(),
            self.process_task_queue(),
            self.periodic_consistency_check(),
        ]
        for task in tasks:
            loop.create_task(task)

    async def fetch_cases(self) -> None:
        logger.info("Starting fetch_cases operation")
        try:
            cases = await self.store.get_all_cases()
            self.current_cases = {
                case_id: Data(**case_data) for case_id, case_data in cases.items()
            }
            logger.info(f"Loaded {len(self.current_cases)} cases successfully.")
        except Exception as e:
            logger.error(f"Error fetching cases: {e}", exc_info=True)

    async def process_task_queue(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                task = await asyncio.wait_for(self.case_task_queue.get(), timeout=1)
                await task()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing task: {e}", exc_info=True)
            finally:
                self.case_task_queue.task_done()

    async def periodic_consistency_check(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=3600)
                if self.shutdown_event.is_set():
                    break
                await self.verify_consistency()
            except asyncio.TimeoutError:
                await self.verify_consistency()
            except Exception as e:
                logger.error(f"Error in periodic consistency check: {e}", exc_info=True)

    async def daily_embed_update_task(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=86400)
                if self.shutdown_event.is_set():
                    break
                await self.update_lawsuit_button_embed()
            except asyncio.TimeoutError:
                await self.update_lawsuit_button_embed()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in daily embed update: {e}", exc_info=True)

    async def verify_consistency(self) -> None:
        logger.info("Starting data consistency check")
        try:
            storage_cases = await self.store.get_all_cases()
            current_case_ids = set(self.current_cases.keys())
            storage_case_ids = set(storage_cases.keys())

            missing_in_storage = current_case_ids - storage_case_ids
            missing_in_memory = storage_case_ids - current_case_ids
            inconsistent = {
                case_id
                for case_id in current_case_ids & storage_case_ids
                if self.current_cases[case_id].dict() != storage_cases[case_id]
            }

            update_tasks = []
            for case_id in missing_in_storage | inconsistent:
                update_tasks.append(
                    self.repository.persist_case(case_id, self.current_cases[case_id])
                )

            for case_id in missing_in_memory:
                self.current_cases[case_id] = Data(**storage_cases[case_id])

            await asyncio.gather(*update_tasks)
            logger.info("Data consistency check completed")
        except Exception as e:
            logger.error(f"Error during consistency check: {e}", exc_info=True)

    async def update_lawsuit_button_embed(self) -> None:
        try:
            if self.lawsuit_button_message_id:
                channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                message = await channel.fetch_message(self.lawsuit_button_message_id)
                daily_proverb = self.view.get_daily_proverb()
                embed = await self.view.create_embed(
                    daily_proverb, "Click the button to file a lawsuit or appeal."
                )
                await message.edit(embeds=[embed])
                logger.info("Lawsuit button embed updated successfully")
        except Exception as e:
            logger.error(f"Error updating lawsuit button embed: {e}", exc_info=True)

    # Command functions

    module_base: Final[interactions.SlashCommand] = interactions.SlashCommand(
        name="lawsuit", description="Lawsuit management system"
    )

    @module_base.subcommand(
        "memory", sub_cmd_description="Print top memory allocations"
    )
    @interactions.check(
        lambda ctx: any(
            role.id == Config.get_instance().JUDGE_ROLE_ID for role in ctx.author.roles
        )
    )
    async def display_memory_allocations(self, ctx: interactions.SlashContext) -> None:
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")[:10]

        stats_content = "\n".join(
            f"{stat.count} {stat.size/1024:.1f} KiB {stat.traceback}"
            for stat in top_stats
        )
        content = f"```py\n{'-' * 30}\n{stats_content}\n```"

        await self.view.send_success(ctx, content, title="Top 10 Memory Allocations")

    @module_base.subcommand(
        "consistency", sub_cmd_description="Perform a data consistency check"
    )
    @interactions.check(
        lambda ctx: any(
            role.id == Config.get_instance().JUDGE_ROLE_ID for role in ctx.author.roles
        )
    )
    async def verify_consistency_command(
        self, ctx: Optional[interactions.SlashContext] = None
    ) -> None:
        if ctx:
            await ctx.defer()

        differences = await self.verify_consistency()
        formatted_results = self.format_consistency_results(differences)

        if ctx:
            await self.view.send_success(
                ctx,
                f"```\n{formatted_results}\n```",
                title="Data Consistency Check Results",
            )
        else:
            logger.info(f"Automated consistency check results:\n{formatted_results}")

    @staticmethod
    def format_consistency_results(differences: Dict[str, Set[str]]) -> str:
        return "\n".join(
            f"{len(cases)} cases {status}: {', '.join(sorted(cases))}"
            for status, cases in differences.items()
            if cases
        )

    @module_base.subcommand(
        "send", sub_cmd_description="Send lawsuit and appeal buttons"
    )
    @interactions.check(
        lambda ctx: any(
            role.id == Config.get_instance().JUDGE_ROLE_ID for role in ctx.author.roles
        )
    )
    async def dispatch_lawsuit_button(self, ctx: interactions.CommandContext) -> None:
        logger.info(f"Attempting to send sue button by user {ctx.author.id}")

        try:
            components = self.view.create_action_buttons()
            daily_proverb = self.view.get_daily_proverb()
            embed = await self.view.create_embed(
                daily_proverb, "Click the button to file a lawsuit or appeal."
            )
            message = await ctx.channel.send(embeds=[embed], components=components)

            self.lawsuit_button_message_id = message.id
            await self.initialize_chat_thread(ctx)
        except HTTPException as e:
            if e.status == 404:
                logger.warning("Interaction not found, possibly due to timeout.")
            else:
                logger.error(f"Error in dispatch_lawsuit_button: {e}", exc_info=True)
                raise

    @module_base.subcommand(
        "role", sub_cmd_description="Assign or revoke a role for a user in a case"
    )
    @interactions.slash_option(
        name="action",
        description="Action to perform",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            interactions.SlashCommandChoice(name="分配", value=UserAction.ASSIGN.value),
            interactions.SlashCommandChoice(name="撤销", value=UserAction.REVOKE.value),
        ],
    )
    @interactions.slash_option(
        name="role",
        description="Role to assign or revoke",
        opt_type=interactions.OptionType.STRING,
        required=True,
        autocomplete=True,
    )
    @interactions.slash_option(
        name="user",
        description="User to assign or revoke the role",
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
        case_id = await self.find_case_number_by_channel_id(ctx.channel_id)
        if not case_id:
            await self.view.send_error(ctx, "Unable to find the associated case.")
            return

        await self.manage_case_role(ctx, case_id, UserAction(action), role, user)

    # Serve functions

    async def handle_case_action(
        self, ctx: interactions.ComponentContext, action: CaseAction, case_id: str
    ) -> None:
        logger.info(f"Handling case action: {action} for case {case_id}")

        async with AsyncExitStack() as stack:
            try:
                case = await stack.enter_async_context(self.case_context(case_id))
                if case is None:
                    await self.send_error(ctx, "Case not found.")
                    return

                guild = await self.bot.fetch_guild(self.config.GUILD_ID)
                member = await guild.fetch_member(ctx.author.id)

                handler = self.case_action_handlers.get(action)
                if handler:
                    await handler(ctx, case_id, case, member)
                else:
                    await self.send_error(ctx, "Invalid action specified.")

            except asyncio.TimeoutError:
                logger.error(f"Timeout acquiring lock for case {case_id}")
                await self.send_error(ctx, "Operation timed out. Please try again.")
            except Exception as e:
                logger.error(f"Error in handle_case_action: {str(e)}", exc_info=True)
                await self.send_error(
                    ctx, "An error occurred while processing your request."
                )

    async def handle_trial_privacy(self, ctx: interactions.ComponentContext) -> None:
        custom_id_parts = ctx.custom_id.split("_")
        if (
            len(custom_id_parts) != 4
            or custom_id_parts[0] != "public"
            or custom_id_parts[1] != "trial"
        ):
            await self.send_error(ctx, "Invalid interaction data.")
            return

        is_public = custom_id_parts[2] == "yes"
        case_id = custom_id_parts[3]

        async with self.case_context(case_id) as case:
            if not case:
                await self.send_error(ctx, "Case not found.")
                return

            await self.create_trial_thread(ctx, case_id, case, is_public)

    async def handle_file_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        user: Union[interactions.Member, interactions.User],
    ) -> None:
        logger.info(f"Starting handle_file_case_request for case {case_id}")

        has_judge_role = await self.user_has_judge_role(user)

        if has_judge_role:
            logger.info(f"User {user.id} has judge role, adding to case {case_id}")
            await self.add_judge_to_case(ctx, case_id, case)
        else:
            logger.info(
                f"User {user.id} does not have judge role, sending permission error"
            )
            await self.view.send_error(ctx, "Insufficient permissions.")

    async def handle_case_closure(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        member: interactions.Member,
        action: CaseAction,
    ):
        if not await self.user_has_permission_for_closure(
            ctx.author.id, case_id, member
        ):
            await self.view.send_error(ctx, "Insufficient permissions.")
            return

        action_name = action.display_name
        new_status = action.new_status

        await self.update_case_status(case_id, case, new_status)
        await self.notify_case_closure(ctx, case_id, action_name)

    async def update_case_status(
        self, case_id: str, case: Data, new_status: CaseStatus
    ) -> None:
        case.status = new_status
        await asyncio.gather(
            self.archive_thread(case.thread_id) if case.thread_id else asyncio.sleep(0),
            (
                self.archive_thread(case.trial_thread_id)
                if case.trial_thread_id
                else asyncio.sleep(0)
            ),
        )

    async def notify_case_closure(
        self, ctx: interactions.ComponentContext, case_id: str, action_name: str
    ) -> None:
        await asyncio.gather(
            self.notify_case_participants(
                case_id, f"第{case_id}号案已被{ctx.author.display_name}{action_name}。"
            ),
            self.view.send_success(ctx, f"第{case_id}号案成功{action_name}。"),
        )

    async def handle_accept_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        member: interactions.Member,
    ):
        if not await self.is_user_assigned_to_case(ctx.author.id, case_id, "judges"):
            await self.view.send_error(ctx, "Insufficient permissions.")
            return

        embed = await self.view.create_embed("是否公开审理")
        buttons = self.view.create_trial_privacy_buttons(case_id)
        await ctx.send(embeds=[embed], components=[interactions.ActionRow(*buttons)])

    async def handle_end_trial(self, ctx: interactions.ComponentContext):
        if match := re.match(r"^end_trial_([a-f0-9]{12})$", ctx.custom_id):
            case_id = match.group(1)
            await self.handle_case_action(ctx, CaseAction.CLOSE, case_id)
        else:
            await self.send_error(ctx, "Invalid interaction data.")

    # Add members to thread

    async def add_members_to_thread(
        self, thread: interactions.GuildText, member_ids: Set[int]
    ) -> None:
        tasks = [
            self._add_member_to_thread(thread, member_id) for member_id in member_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for member_id, result in zip(member_ids, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to add user {member_id} to thread: {str(result)}")

    @staticmethod
    async def _add_member_to_thread(
        thread: interactions.GuildText, member_id: int
    ) -> None:
        user = await Lawsuit.bot.fetch_user(member_id)
        await thread.add_member(user)

    async def add_participants_to_thread(
        self, thread: interactions.GuildText, case: Data
    ) -> None:
        guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        judges = await self.get_judges(guild)
        participants = {case.plaintiff_id, case.defendant_id, *judges}
        await self.add_members_to_thread(thread, participants)

    async def add_judge_to_case(
        self, ctx: interactions.ComponentContext, case_id: str, case: Data
    ) -> None:
        logger.info(f"Starting add_judge_to_case for case {case_id}")
        try:
            async with self.case_lock_context(case_id):
                if len(case.judges) >= self.config.MAX_JUDGES_PER_CASE:
                    await self.view.send_error(
                        ctx,
                        f"This case already has the maximum number of judges ({self.config.MAX_JUDGES_PER_CASE}).",
                    )
                    return

                if ctx.author.id not in case.judges:
                    new_judges = case.judges + (ctx.author.id,)
                    await self.repository.update_case(case_id, judges=new_judges)

            notification = f"{ctx.author.display_name}以法官身份参与第{case_id}号案。"

            await asyncio.gather(
                self.view.send_success(ctx, f"您以法官身份参与第{case_id}号案。"),
                self.notify_case_participants(case_id, notification),
            )
        except Exception as e:
            logger.exception(f"Error in add_judge_to_case for case {case_id}")
            await self.view.send_error(
                ctx, "An error occurred while processing your request."
            )

    # User actions

    async def handle_user_message_action(
        self, ctx: interactions.ComponentContext
    ) -> None:
        try:
            action, target_id = self.parse_custom_id(ctx.custom_id)
            case_id = await self.validate_user_permissions(ctx)
            await self.perform_user_action(ctx, case_id, target_id, action)
        except (ValueError, PermissionError) as e:
            await self.send_error(ctx, str(e))
        except Exception as e:
            logger.error(
                f"Unexpected error in handle_user_message_action: {e}", exc_info=True
            )
            await self.send_error(ctx, "An unexpected error occurred.")

    @staticmethod
    def parse_custom_id(custom_id: str) -> Tuple[UserAction, int]:
        if match := re.match(r"^(mute|unmute|pin|delete)_(\d{1,20})$", custom_id):
            action, target_id = match.groups()
            return UserAction[action.upper()], int(target_id)
        raise ValueError(f"Invalid custom_id format: {custom_id}")

    async def validate_user_permissions(
        self, ctx: interactions.ComponentContext
    ) -> str:
        case_id = self.find_case_number_by_channel_id(ctx.channel_id)
        if not case_id or not await self.is_user_assigned_to_case(
            ctx.author.id, case_id, "judges"
        ):
            raise PermissionError("Insufficient permissions.")
        return case_id

    async def perform_user_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        action: UserAction,
    ) -> None:
        handler = self.get_action_handler(action)
        if not handler:
            raise ValueError(f"Invalid action: {action}")

        await handler(
            ctx,
            case_id,
            target_id,
            mute=(
                (action == UserAction.MUTE)
                if action in {UserAction.MUTE, UserAction.UNMUTE}
                else None
            ),
            action_type=(
                action.name.lower()
                if action in {UserAction.PIN, UserAction.DELETE}
                else None
            ),
        )

    async def _handle_mute_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        mute: bool,
    ) -> None:
        action_str: Final[Literal["muted", "unmuted"]] = "muted" if mute else "unmuted"

        async with self.case_lock_context(case_id):
            case = await self.repository.get_case(case_id)
            is_muted = target_id in case.mute_list

            if (mute and is_muted) or (not mute and not is_muted):
                await self.view.send_error(
                    ctx, f"User <@{target_id}> is already {action_str}."
                )
                return

            new_mute_list = set(case.mute_list) ^ {target_id}
            await self.repository.update_case(case_id, mute_list=new_mute_list)

        await asyncio.gather(
            self.view.send_success(ctx, f"User <@{target_id}> has been {action_str}."),
            self.notify_case_participants(
                case_id,
                f"User <@{target_id}> has been {action_str} by <@{ctx.author.id}>.",
            ),
        )

    async def _handle_message_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        action_type: Literal["pin", "delete"],
    ) -> None:
        try:
            channel = await self.bot.fetch_channel(ctx.channel_id)
            message = await channel.fetch_message(target_id)

            action_method = getattr(message, action_type)
            await action_method()

            action_str = f"{action_type}ned"
            await self.view.send_success(ctx, f"Message has been {action_str}.")

        except NotFound:
            await self.view.send_error(ctx, f"Message not found in case {case_id}.")
        except Forbidden:
            await self.view.send_error(
                ctx, f"I don't have permission to {action_type} messages."
            )
        except HTTPException as e:
            await self.view.send_error(
                ctx, f"An error occurred while {action_type}ing the message: {str(e)}"
            )

    # Notify functions

    async def notify_evidence_decision(
        self, user_id: int, case_id: str, decision: str
    ) -> None:
        embed = await self.view.create_embed(
            "Evidence Decision",
            f"Your evidence for Case {case_id} has been {decision}.",
        )
        await self._send_dm_with_fallback(user_id, embed=embed)

    async def notify_case_participants(self, case_id: str, message: str) -> None:
        case = await self.repository.get_case(case_id)
        if case:
            participants = {case.plaintiff_id, case.defendant_id, *case.judges}
            embed = await self.view.create_embed(f"Case {case_id} Update", message)
            await asyncio.gather(
                *(
                    self._send_dm_with_fallback(participant_id, embed=embed)
                    for participant_id in participants
                )
            )

    async def notify_judges_of_new_evidence(self, case_id: str, user_id: int) -> None:
        case = await self.repository.get_case(case_id)
        if not case:
            logger.warning(f"Case {case_id} not found for evidence notification.")
            return

        embed = await self.view.create_embed(
            "Evidence Submission",
            f"New evidence has been submitted for Case {case_id}. Please decide whether to make it public.",
        )
        buttons = self.view.create_evidence_action_buttons(case_id, user_id)

        await self._notify_users(
            case.judges,
            partial(
                self._send_dm_with_fallback,
                embed=embed,
                components=[interactions.ActionRow(*buttons)],
            ),
        )

    async def notify_judges(self, case_id: str, guild_id: int) -> None:
        guild = await self.bot.fetch_guild(guild_id)
        judge_role = guild.get_role(self.config.JUDGE_ROLE_ID)

        if not judge_role:
            logger.warning(
                f"Judge role with ID {self.config.JUDGE_ROLE_ID} not found in guild {guild_id}."
            )
            return

        judge_members = [
            member for member in guild.members if judge_role in member.roles
        ]

        if not judge_members:
            logger.warning(
                f"No judges found with role ID {self.config.JUDGE_ROLE_ID} for case {case_id}."
            )
            return

        case = await self.repository.get_case(case_id)
        if case and case.thread_id:
            thread_link = f"https://discord.com/channels/{guild_id}/{case.thread_id}"

            notify_judges_button = interactions.Button(
                custom_id=f"file_{case_id}",
                style=interactions.ButtonStyle.PRIMARY,
                label="参与调解",
            )
            notification_embed = await self.view.create_embed(
                f"本院收悉第{case_id}号案诉状",
                f"请前往[调解室]({thread_link})参与调解。",
            )

            await self._notify_users(
                (member.id for member in judge_members),
                partial(
                    self._send_dm_with_fallback,
                    embed=notification_embed,
                    components=[notify_judges_button],
                ),
            )

    # Helper methods

    async def _send_dm_with_fallback(self, user_id: int, **kwargs) -> None:
        try:
            user = await self.bot.fetch_user(user_id)
            await user.send(**kwargs)
        except HTTPException as e:
            logger.warning(f"Failed to send DM to user {user_id}: {e}")

    async def _notify_users(
        self, user_ids: Iterable[int], notification_func: Callable
    ) -> None:
        async def notify(user_id: int) -> None:
            with suppress(Exception):
                await notification_func(user_id)

        await asyncio.gather(*(notify(user_id) for user_id in user_ids))

    # Role functions

    @manage_role.autocomplete("role")
    async def role_autocomplete(
        self, ctx: interactions.AutocompleteContext
    ) -> List[interactions.Choice]:
        focused_option = ctx.focused
        return [
            interactions.Choice(name=role.value, value=role.name)
            for role in CaseRole
            if focused_option.lower() in role.value.lower()
        ][:25]

    async def manage_case_role(
        self,
        ctx: interactions.SlashContext,
        case_id: str,
        action: UserAction,
        role: str,
        user: interactions.User,
    ) -> None:
        async with self.case_lock_context(case_id):
            try:
                case = await self.repository.get_case(case_id)
                if case is None:
                    raise ValueError(f"Case {case_id} not found.")

                if not await self.is_user_assigned_to_case(
                    ctx.author.id, case_id, "judges"
                ):
                    raise PermissionError(
                        "Insufficient permissions to manage case roles."
                    )

                try:
                    case_role = CaseRole[role]
                except KeyError:
                    raise ValueError(f"Invalid role: {role}")

                roles = case.roles.copy()
                role_users = roles.setdefault(case_role.name, set())

                if action == UserAction.ASSIGN:
                    if user.id in role_users:
                        raise ValueError(f"User already has the role {case_role.value}")
                    role_users.add(user.id)
                    action_str = "Assigned"
                elif action == UserAction.REVOKE:
                    if user.id not in role_users:
                        raise ValueError(
                            f"User doesn't have the role {case_role.value}"
                        )
                    role_users.remove(user.id)
                    action_str = "Revoked"
                else:
                    raise ValueError(f"Invalid action: {action}")

                await self.repository.update_case(case_id, roles=roles)

                notification_message = (
                    f"<@{ctx.author.id}> has {action_str.lower()} the role of {case_role.value} "
                    f"{'to' if action == UserAction.ASSIGN else 'from'} <@{user.id}>."
                )

                await asyncio.gather(
                    self.view.send_success(
                        ctx,
                        f"{action_str} role {case_role.value} "
                        f"{'to' if action == UserAction.ASSIGN else 'from'} <@{user.id}>",
                    ),
                    self.notify_case_participants(case_id, notification_message),
                )

            except (ValueError, PermissionError) as e:
                await self.view.send_error(ctx, str(e))
            except asyncio.TimeoutError:
                logger.error(f"Timeout acquiring case_lock for case {case_id}")
                await self.view.send_error(
                    ctx, "Operation timed out. Please try again."
                )
            except Exception as e:
                logger.error(
                    f"Error in manage_case_role for case {case_id}: {str(e)}",
                    exc_info=True,
                )
                await self.view.send_error(
                    ctx, "An unexpected error occurred while processing your request."
                )

    # Evidence functions

    async def fetch_attachment_as_file(
        self, attachment: Dict[str, str]
    ) -> Optional[interactions.File]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(attachment["url"], timeout=10) as response:
                    if response.status == 200:
                        content = await response.read()
                        return interactions.File(
                            io.BytesIO(content),
                            filename=attachment["filename"],
                            description=f"Evidence attachment for {attachment['filename']}",
                        )
                    else:
                        logger.warning(f"Failed to fetch attachment: {response.status}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching attachment: {attachment['url']}")
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching attachment: {str(e)}")
        return None

    async def handle_judge_evidence_action(
        self, ctx: interactions.ComponentContext
    ) -> None:
        if not (
            match := re.match(
                r"(approve|reject)_evidence_([a-f0-9]{12})_(\d+)", ctx.custom_id
            )
        ):
            logger.warning(f"Invalid custom_id format: {ctx.custom_id}")
            await self.view.send_error(ctx, "Invalid evidence action.")
            return

        action, case_id, user_id = match.groups()
        await self.process_judge_evidence_decision(ctx, action, case_id, int(user_id))

    async def process_judge_evidence_decision(
        self,
        ctx: interactions.ComponentContext,
        action: Literal["approve", "reject"],
        case_id: str,
        user_id: int,
    ) -> None:
        async with self.case_lock_context(case_id):
            case = await self.repository.get_case(case_id)
            if not case:
                await self.view.send_error(ctx, "Case not found.")
                return

            evidence = next(
                (e for e in case.evidence_queue if e.user_id == user_id), None
            )
            if not evidence:
                await self.view.send_error(ctx, "Evidence not found.")
                return

            new_state = (
                EvidenceAction.APPROVED
                if action == "approve"
                else EvidenceAction.REJECTED
            )
            evidence.state = new_state

            evidence_list = f"evidence_{new_state.name.lower()}"
            getattr(case, evidence_list, []).append(evidence)
            case.evidence_queue = [
                e for e in case.evidence_queue if e.user_id != user_id
            ]

            await self.repository.update_case(
                case_id,
                evidence_queue=case.evidence_queue,
                **{evidence_list: getattr(case, evidence_list)},
            )

        await asyncio.gather(
            self.notify_evidence_decision(user_id, case_id, new_state.name.lower()),
            self.notify_case_participants(
                case_id,
                f"Evidence from <@{user_id}> has been {new_state.name.lower()} by <@{ctx.author.id}>.",
            ),
            (
                self.publish_approved_evidence
                if new_state == EvidenceAction.APPROVED
                else self.handle_rejected_evidence
            )(ctx, case, evidence),
            self.view.send_success(
                ctx, f"Evidence has been successfully {new_state.name.lower()}."
            ),
        )

    async def publish_approved_evidence(
        self, ctx: interactions.ComponentContext, case: Data, evidence: Evidence
    ) -> None:
        if not case.trial_thread_id:
            await self.view.send_success(
                ctx,
                "The evidence has been approved and will be made public when the trial begins.",
            )
            return

        trial_thread = await self.bot.fetch_channel(case.trial_thread_id)
        content = f"Evidence submitted by <@{evidence.user_id}>:\n{evidence.content}"

        files = await asyncio.gather(
            *(
                self.fetch_attachment_as_file(attachment)
                for attachment in evidence.attachments
            )
        )
        files = [file for file in files if file]

        await asyncio.gather(
            trial_thread.send(content=content, files=files),
            self.view.send_success(
                ctx, "The evidence has been made public and sent to the trial thread."
            ),
        )

    async def handle_rejected_evidence(
        self, ctx: interactions.ComponentContext, case: Data, evidence: Evidence
    ) -> None:
        user = await self.bot.fetch_user(evidence.user_id)
        await asyncio.gather(
            user.send(
                "Your submitted evidence has been decided not to be made public."
            ),
            self.view.send_success(
                ctx, "The evidence has been decided not to be made public."
            ),
        )

    async def handle_direct_message_evidence(
        self, message: interactions.Message
    ) -> None:
        try:
            case_id = await self.get_user_active_case_number(message.author.id)
            if case_id is None:
                await message.author.send(
                    "You don't have any active cases at the moment."
                )
                return

            evidence = self.create_evidence_dict(message, self.config.MAX_FILE_SIZE)
            await asyncio.gather(
                self.process_evidence(case_id, evidence),
                message.author.send("Evidence received and processed."),
            )
        except HTTPException as e:
            log_message = (
                f"Cannot send DM to user {message.author.id}. They may have DMs disabled."
                if e.status == 403
                else f"HTTP error when sending DM to user {message.author.id}: {str(e)}"
            )
            (
                logger.warning(log_message)
                if e.status == 403
                else logger.error(log_message)
            )

    async def handle_channel_message(self, message: interactions.Message) -> None:
        if case_id := self.find_case_number_by_channel_id(message.channel.id):
            case = await self.repository.get_case(case_id)
            if case and message.author.id in case.mute_list:
                try:
                    await message.delete()
                except Forbidden:
                    logger.warning(
                        f"No permission to delete messages in channel {message.channel.id}."
                    )
                except HTTPException as e:
                    logger.error(f"HTTP error when deleting message: {str(e)}")

    def create_evidence_dict(
        self, message: interactions.Message, max_file_size: int
    ) -> Evidence:
        return Evidence(
            user_id=message.author.id,
            content=self.sanitize_user_input(message.content),
            attachments=[
                {
                    "url": attachment.url,
                    "filename": self.sanitize_user_input(attachment.filename),
                    "content_type": attachment.content_type,
                }
                for attachment in message.attachments
                if self.is_valid_attachment(attachment, max_file_size)
            ],
            message_id=message.id,
            timestamp=int(message.created_at.timestamp()),
            state=EvidenceAction.PENDING,
        )

    async def process_evidence(self, case_id: str, evidence: Evidence) -> None:
        async with AsyncExitStack() as stack:
            try:
                await stack.enter_async_context(self.case_lock_context(case_id))

                case = await self.repository.get_case(case_id)

                if case is None:
                    raise ValueError(f"Case {case_id} does not exist.")

                VALID_STATUSES: Final = frozenset(
                    {CaseStatus.FILED, CaseStatus.IN_PROGRESS}
                )
                if case.status not in VALID_STATUSES:
                    raise ValueError(
                        f"Case {case_id} is not in a valid state for evidence submission."
                    )

                updated_queue = [*case.evidence_queue, evidence]

                await asyncio.gather(
                    self.repository.update_case(case_id, evidence_queue=updated_queue),
                    asyncio.wait_for(
                        self.notify_judges_of_new_evidence(
                            case_id, evidence["user_id"]
                        ),
                        timeout=5,
                    ),
                )

            except asyncio.TimeoutError:
                logger.warning(f"Notification timeout for case {case_id}")
            except Exception as e:
                logger.critical(
                    f"Unexpected error processing evidence for case {case_id}: {str(e)}",
                    exc_info=True,
                )
                raise

    # Check functions

    async def user_has_judge_role(
        self, user: Union[interactions.Member, interactions.User]
    ) -> bool:
        if isinstance(user, interactions.Member):
            return self.user_has_role(user, self.config.JUDGE_ROLE_ID)
        else:
            guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            member = await guild.fetch_member(user.id)
            return self.user_has_role(member, self.config.JUDGE_ROLE_ID)

    async def user_has_permission_for_closure(
        self, user_id: int, case_id: str, member: interactions.Member
    ) -> bool:
        return await self.is_user_assigned_to_case(
            user_id, case_id, "plaintiff_id"
        ) or self.user_has_role(member, self.config.JUDGE_ROLE_ID)

    def is_valid_attachment(
        self, attachment: interactions.Attachment, max_size: int
    ) -> bool:
        return (
            attachment.content_type in self.config.ALLOWED_MIME_TYPES
            and attachment.size <= max_size
        )

    @staticmethod
    def user_has_role(member: interactions.Member, role_id: int) -> bool:
        return role_id in member.role_ids

    @lru_cache(maxsize=1024, typed=True)
    def get_role_check(self, role: str) -> Callable[[Data, int], bool]:
        role_checks: Final[Dict[str, Callable[[Data, int], bool]]] = {
            "plaintiff_id": operator.attrgetter("plaintiff_id").__eq__,
            "defendant_id": operator.attrgetter("defendant_id").__eq__,
            "judges": lambda c, user_id: user_id in c.judges,
        }
        return role_checks.get(role) or (
            lambda c, user_id: user_id in getattr(c, role, frozenset())
        )

    async def is_user_assigned_to_case(
        self, user_id: int, case_id: str, role: str
    ) -> bool:
        case = await self.repository.get_case(case_id)
        return case is not None and self.get_role_check(role)(case, user_id)

    async def has_ongoing_lawsuit(self, user_id: int) -> bool:
        return any(
            case.plaintiff_id == user_id and case.status is not CaseStatus.CLOSED
            for case in self.current_cases.values()
        )

    @staticmethod
    def validate_and_sanitize(responses: Dict[str, str]) -> Tuple[bool, Dict[str, str]]:
        sanitized_data: Dict[str, str] = {
            key: Lawsuit.sanitize_user_input(value) for key, value in responses.items()
        }
        return len(sanitized_data) == 3 and all(sanitized_data.values()), sanitized_data

    async def validate_defendant(
        self, data: Dict[str, str]
    ) -> Tuple[bool, Optional[int]]:
        try:
            defendant_id = int(data["defendant_id"])
            await self.bot.fetch_user(defendant_id)
            return True, defendant_id
        except (ValueError, NotFound) as e:
            logger.error(f"Invalid defendant ID: {data['defendant_id']}. Error: {e}")
            return False, None

    # Utility functions

    async def archive_thread(self, thread_id: int):
        try:
            thread = await self.bot.fetch_channel(thread_id)
            await thread.edit(archived=True, locked=True)
        except Exception as e:
            logger.error(f"Failed to archive thread {thread_id}: {str(e)}")

    @staticmethod
    def sanitize_user_input(text: str, max_length: int = 2000) -> str:
        ALLOWED_TAGS: Final[frozenset] = frozenset()
        ALLOWED_ATTRIBUTES: Final[Dict[str, frozenset]] = {}
        WHITESPACE_PATTERN: Final = re.compile(r"\s+")

        cleaner: Final = functools.partial(
            bleach.clean,
            tags=ALLOWED_TAGS,
            attributes=ALLOWED_ATTRIBUTES,
            strip=True,
            filters=[bleach.sanitizer.ALLOWED_PROTOCOLS.__contains__],
        )

        return WHITESPACE_PATTERN.sub(" ", cleaner(text)).strip()[:max_length]

    async def delete_thread_created_message(
        self, channel_id: int, thread: interactions.GuildText
    ):
        try:
            parent_channel = await self.bot.fetch_channel(channel_id)
            messages = await parent_channel.fetch_messages(limit=10)
            thread_created_message = next(
                (
                    m
                    for m in messages
                    if m.type == interactions.MessageType.THREAD_CREATED
                    and m.thread
                    and m.thread.id == thread.id
                ),
                None,
            )
            if thread_created_message:
                with suppress(Exception):
                    await thread_created_message.delete()
        except Exception as e:
            logger.error(f"Failed to delete thread created message: {str(e)}")

    async def initialize_chat_thread(self, ctx: interactions.CommandContext) -> None:
        try:
            channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            if not isinstance(channel, interactions.GuildText):
                raise ValueError("Invalid courtroom channel type")

            thread = await self.get_or_create_chat_thread(channel)
            embed = await self.create_chat_room_setup_embed(thread)
            await self.view.send_success(ctx, embed.description)
        except Exception as e:
            logger.error(f"Chat thread initialization error: {str(e)}")
            await self.view.send_error(ctx, f"An error occurred: {str(e)}")

    async def setup_mediation_thread(self, case_id: str, case: Data) -> bool:
        try:
            channel = await self.get_courtroom_channel()
            thread = await self.create_mediation_thread(channel, case_id)
            await self.send_case_summary(thread, case_id, case)
            await self.update_case_with_thread(case_id, thread.id)
            await self.add_participants_to_thread(thread, case)
            return True
        except Exception as e:
            logger.error(f"Failed to create mediation thread: {str(e)}")
            return False

    async def send_case_summary(
        self, thread: interactions.GuildText, case_id: str, case: Data
    ) -> None:
        embed = await self.view.create_case_summary_embed(case_id, case)
        components = self.view.create_case_action_buttons(case_id)
        await thread.send(embeds=[embed], components=components)

    async def update_case_with_thread(self, case_id: str, thread_id: int) -> None:
        await self.repository.update_case(case_id, thread_id=thread_id)

    # Create functions

    async def create_mediation_thread(
        self, channel: interactions.GuildText, case_id: str
    ) -> interactions.GuildText:
        return await channel.create_thread(
            name=f"第{case_id}号调解室",
            thread_type=interactions.ChannelType.GUILD_PRIVATE_THREAD,
        )

    @staticmethod
    @lru_cache(maxsize=1024, typed=True)
    def create_case_id(length: int = 6) -> str:
        return secrets.token_hex(length // 2)

    async def create_new_case(
        self, ctx: interactions.ModalContext, data: Dict[str, str], defendant_id: int
    ) -> Tuple[Optional[str], Optional[Data]]:
        try:
            case_id = self.create_case_id()
            case = self.create_case_data(ctx, data, defendant_id)
            await self.repository.persist_case(case_id, case)
            return case_id, case
        except Exception as e:
            logger.error(f"Error creating new case: {str(e)}")
            return None, None

    def create_case_data(
        self, ctx: interactions.ModalContext, data: Dict[str, str], defendant_id: int
    ) -> Data:
        return Data(
            plaintiff_id=ctx.author.id,
            defendant_id=defendant_id,
            accusation=data.get("accusation", ""),
            facts=data.get("facts", ""),
            status=CaseStatus.FILED,
            judges=[],
        )

    async def create_new_chat_thread(
        self, channel: interactions.GuildText
    ) -> interactions.GuildText:
        thread = await channel.create_public_thread(
            name="聊天室", auto_archive_duration=10080
        )
        await thread.edit(rate_limit_per_user=1)
        return thread

    async def create_chat_room_setup_embed(
        self, thread: interactions.GuildText
    ) -> interactions.Embed:
        return await self.view.create_embed(
            "Chat Room Setup",
            f"The lawsuit and appeal buttons have been sent to this channel. "
            f"The chat room thread is ready: {thread.jump_url}",
        )

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
        thread_name = f"第{case_id}号{'公开' if is_public else '秘密'}审判庭"

        channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
        trial_thread = await channel.create_thread(
            name=thread_name, thread_type=thread_type
        )

        embed = await self.view.create_case_summary_embed(case_id, case)
        end_trial_button = self.view.create_end_trial_button(case_id)
        await trial_thread.send(embeds=[embed], components=[end_trial_button])

        allowed_roles = await self.get_allowed_roles(ctx.guild)
        case.trial_thread_id = trial_thread.id
        case.allowed_roles = allowed_roles

        if is_public:
            await self.delete_thread_created_message(
                self.config.COURTROOM_CHANNEL_ID, trial_thread
            )

        await asyncio.gather(
            self.view.send_success(
                ctx, f"第{case_id}号案决定{'公开' if is_public else '秘密'}审理。"
            ),
            self.notify_case_participants(
                case_id, f"第{case_id}号案决定{'公开' if is_public else '秘密'}审理。"
            ),
            (
                self.add_members_to_thread(trial_thread, case.judges)
                if case.trial_thread_id
                else asyncio.sleep(0)
            ),
        )

    # Get functions

    async def get_allowed_roles(self, guild: interactions.Guild) -> List[int]:
        role_names = [
            "公诉人",
            "自诉人",
            "原告人",
            "被告人",
            "辩护人",
            "代理人",
            "被害人",
            "证人",
        ]
        return [role.id for role in guild.roles if role.name in role_names]

    @lru_cache(maxsize=128)
    def get_action_handler(self, action: UserAction) -> Callable:
        return {
            UserAction.MUTE: self._handle_mute_action,
            UserAction.UNMUTE: self._handle_mute_action,
            UserAction.PIN: self._handle_message_action,
            UserAction.DELETE: self._handle_message_action,
        }.get(action, lambda *args, **kwargs: None)

    @lru_cache(maxsize=1)
    async def get_judges(self, guild: interactions.Guild) -> Set[int]:
        return {
            member.id
            for member in guild.members
            if self.config.JUDGE_ROLE_ID in member.role_ids
        }

    async def get_courtroom_channel(self) -> interactions.GuildText:
        channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
        if not isinstance(channel, interactions.GuildText):
            raise ValueError(f"Invalid channel type: {type(channel)}")
        return channel

    @classmethod
    @lru_cache(maxsize=128, typed=True)
    def get_sanitizer(cls, max_length: int = 2000) -> Callable[[str], str]:
        return functools.partial(cls.sanitize_user_input, max_length=max_length)

    async def get_or_create_chat_thread(
        self, channel: interactions.GuildText
    ) -> interactions.GuildText:
        existing_thread = await self.find_existing_chat_thread(channel)
        if existing_thread:
            return existing_thread
        return await self.create_new_chat_thread(channel)

    async def find_existing_chat_thread(
        self, channel: interactions.GuildText
    ) -> Optional[interactions.GuildText]:
        return next(
            (t for t in await channel.fetch_active_threads() if t.name == "聊天室"),
            None,
        )

    @cached(cache=TTLCache(maxsize=1024, ttl=60))
    async def get_case(self, case_id: str) -> Optional[Data]:
        return await self.repository.get_case(case_id)

    @lru_cache(maxsize=128)
    async def get_user_active_case_number(self, user_id: int) -> Optional[str]:
        return next(
            (
                case_id
                for case_id, case in self.current_cases.items()
                if user_id in {case.plaintiff_id, case.defendant_id}
                and case.status != CaseStatus.CLOSED
            ),
            None,
        )

    @lru_cache(maxsize=128)
    def find_case_number_by_channel_id(self, channel_id: int) -> Optional[str]:
        return next(
            (
                case_id
                for case_id, case in self.current_cases.items()
                if channel_id in {case.thread_id, case.trial_thread_id}
            ),
            None,
        )

    # Lock functions

    @asynccontextmanager
    async def case_lock_context(self, case_id: str) -> AsyncGenerator[None, None]:
        lock = self._case_locks.setdefault(case_id, asyncio.Lock())
        start_time = perf_counter()
        try:
            await asyncio.wait_for(lock.acquire(), timeout=5.0)
            self._lock_timestamps[case_id] = asyncio.get_event_loop().time()
            yield
        except TimeoutError:
            logger.error(f"Timeout acquiring lock for case {case_id}")
            raise
        finally:
            if lock.locked():
                lock.release()
                duration = perf_counter() - start_time
                logger.info(
                    f"Released lock for case {case_id} after {duration:.2f} seconds"
                )

    @lru_cache(maxsize=1000)
    def get_case_lock(self, case_id: str) -> asyncio.Lock:
        return self.case_lock_cache.setdefault(case_id, asyncio.Lock())

    @asynccontextmanager
    async def case_context(self, case_id: str):
        async with self.get_case_lock(case_id):
            case = await self.get_case(case_id)
            yield case
            if case:
                await self.repository.persist_case(case_id, case)
