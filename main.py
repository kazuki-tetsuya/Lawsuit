from __future__ import annotations

import asyncio
import functools
import io
import logging
import logging.handlers
import os
import random
import re
import secrets
import tracemalloc
from asyncio import Lock, TimeoutError
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum, IntEnum
from functools import cached_property, lru_cache, partial, wraps
from logging.handlers import RotatingFileHandler
from time import perf_counter
from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
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
    runtime_checkable,
)

import aiodns  # DNS resolution for asyncio
import aiohttp
import bleach
import cchardet  # Character encoding detection
import interactions
import lmdb
import orjson
import uvloop
from asyncache import cached
from cachetools import TTLCache
from cytoolz import compose, curry, memoize
from frozendict import frozendict  # Immutable dictionary for faster lookups
from interactions.api.events import ExtensionUnload, MessageCreate, NewThreadCreate
from interactions.client.errors import Forbidden, HTTPException, NotFound
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    StrictStr,
    ValidationError,
    constr,
    root_validator,
    validator,
)
from sortedcontainers import SortedDict, SortedList, SortedSet  # Sorted collections

T = TypeVar("T")
P = ParamSpec("P")
ContextVarLock = Dict[str, Lock]
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

BASE_DIR: Final[str] = os.path.dirname(os.path.abspath(__file__))
LOG_FILE: Final[str] = os.path.join(BASE_DIR, "lawsuit.log")

logger: Final[logging.Logger] = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler: Final[RotatingFileHandler] = RotatingFileHandler(
    LOG_FILE, maxBytes=1 * 1024 * 1024, backupCount=5
)
file_handler.setLevel(logging.DEBUG)

formatter: Final[logging.Formatter] = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# Model


@dataclass(frozen=True)
class Config:
    GUILD_ID: Final[int] = 1150630510696075404
    JUDGE_ROLE_ID: Final[int] = 1200100104682614884
    PLAINTIFF_ROLE_ID: Final[int] = 1200043628899356702
    COURTROOM_CHANNEL_ID: Final[int] = 1247290032881008701
    MAX_JUDGES_PER_CASE: Final[int] = 1
    MAX_JUDGES_PER_APPEAL: Final[int] = 3
    MAX_FILE_SIZE: Final[int] = 10 * 1024 * 1024
    ALLOWED_MIME_TYPES: Final[FrozenSet[str]] = field(
        default_factory=lambda: frozenset(
            {"image/jpeg", "image/png", "application/pdf", "text/plain"}
        )
    )


class CaseStatus(Enum):
    FILED = "FILED"
    IN_PROGRESS = "IN_PROGRESS"
    CLOSED = "CLOSED"

    def __str__(self) -> str:
        return self.value


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
    PROSECUTOR = "Prosecutor"
    PRIVATE_PROSECUTOR = "Private Prosecutor"
    PLAINTIFF = "Plaintiff"
    AGENT = "Agent"
    DEFENDANT = "Defendant"
    DEFENDER = "Defender"
    WITNESS = "Witness"

    def __str__(self) -> str:
        return self.value


class EvidenceAction(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"

    def __str__(self) -> str:
        return self.value


class InteractionAction(Enum):
    FILE = "file"
    CLOSE = "close"
    WITHDRAW = "withdraw"
    ACCEPT = "accept"
    DISMISS = "dismiss"
    PUBLIC_TRIAL = "public_trial"
    END_TRIAL = "end_trial"
    MUTE = "mute"
    UNMUTE = "unmute"
    PIN = "pin"
    DELETE = "delete"
    APPROVE_EVIDENCE = "approve_evidence"
    REJECT_EVIDENCE = "reject_evidence"

    @cached_property
    def display_name(self) -> str:
        display_names: Final[dict[InteractionAction, str]] = {
            InteractionAction.FILE: "File",
            InteractionAction.CLOSE: "Close",
            InteractionAction.WITHDRAW: "Withdraw",
            InteractionAction.ACCEPT: "Accept",
            InteractionAction.DISMISS: "Dismiss",
        }
        return display_names[self]

    @cached_property
    def new_status(self) -> CaseStatus:
        status_mapping: Final[dict[InteractionAction, CaseStatus]] = {
            InteractionAction.WITHDRAW: CaseStatus.CLOSED,
            InteractionAction.CLOSE: CaseStatus.CLOSED,
            InteractionAction.ACCEPT: CaseStatus.IN_PROGRESS,
            InteractionAction.DISMISS: CaseStatus.CLOSED,
            InteractionAction.FILE: CaseStatus.FILED,
        }
        return status_mapping[self]

    def __str__(self) -> str:
        return self.value


class Attachment(BaseModel):
    url: StrictStr = Field(..., description="URL of the attachment")
    filename: StrictStr = Field(..., description="Filename of the attachment")
    content_type: StrictStr = Field(..., description="MIME type of the attachment")

    @validator("*", pre=True)
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class Evidence(BaseModel):
    user_id: PositiveInt = Field(
        ...,
        description="User ID of the evidence submitter, must be a positive integer",
        gt=0,
    )
    content: Annotated[str, constr(min_length=1, max_length=4000)] = Field(
        ...,
        description="Content of the evidence, between 1 and 4000 characters",
        regex=r"^(?!\s*$).+",
    )
    attachments: Tuple[Attachment, ...] = Field(
        default_factory=tuple,
        description="Tuple of attachment metadata",
        max_items=10,
    )
    message_id: PositiveInt = Field(
        ...,
        description="Discord message ID of the evidence submission, must be a positive integer",
        gt=0,
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of evidence submission",
    )
    state: EvidenceAction = Field(
        default=EvidenceAction.PENDING,
        description="Current state of the evidence",
    )

    model_config = ConfigDict(
        frozen=True,
        validate_assignment=True,
        anystr_strip_whitespace=True,
        use_enum_values=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "user_id": 123456789101112131,
                    "content": "This is a piece of evidence.",
                    "message_id": 1234567891011121314,
                }
            ]
        },
    )

    @validator("timestamp")
    def ensure_utc(cls, v: datetime) -> datetime:
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v

    @validator("attachments", pre=True, each_item=False)
    def validate_attachments(cls, v: Any) -> Tuple[Attachment, ...]:
        if isinstance(v, (list, tuple)):
            return tuple(
                Attachment.model_validate(item) if isinstance(item, dict) else item
                for item in v
            )
        raise ValueError(
            "Attachments must be a list or tuple of dictionaries representing Attachment objects"
        )

    @property
    def total_attachment_size(self) -> int:
        return sum(len(attachment.url) for attachment in self.attachments)

    def __str__(self) -> str:
        return f"Evidence(user_id={self.user_id}, content='{self.content[:50]}...', attachments={len(self.attachments)})"

    def __repr__(self) -> str:
        return self.__str__()

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Evidence":
        return cls.model_validate(data)


class Data(BaseModel):
    plaintiff_id: PositiveInt = Field(..., description="ID of the plaintiff")
    defendant_id: PositiveInt = Field(..., description="ID of the defendant")
    status: CaseStatus = Field(
        CaseStatus.FILED, description="Current status of the case"
    )
    thread_id: Optional[PositiveInt] = Field(
        None, description="Thread ID associated with the case"
    )
    judges: FrozenSet[int] = Field(
        default_factory=frozenset, description="Set of judge IDs"
    )
    trial_thread_id: Optional[PositiveInt] = Field(None, description="Trial thread ID")
    allowed_roles: FrozenSet[int] = Field(
        default_factory=frozenset, description="Roles allowed in the trial thread"
    )
    log_message_id: Optional[PositiveInt] = Field(None, description="Log message ID")
    mute_list: FrozenSet[int] = Field(
        default_factory=frozenset, description="Set of user IDs who are muted"
    )
    roles: Dict[str, FrozenSet[int]] = Field(
        default_factory=dict, description="Roles assigned within the case"
    )
    evidence_queue: Tuple[Evidence, ...] = Field(
        default_factory=tuple, description="Queue of evidence submissions"
    )
    accusation: Optional[Annotated[str, constr(max_length=1000)]] = Field(
        None, description="Accusation details"
    )
    facts: Optional[Annotated[str, constr(max_length=2000)]] = Field(
        None, description="Facts of the case"
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp of case creation",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp of last update",
    )

    WHITESPACE_REGEX: re.Pattern = re.compile(r"\s+")

    class Config:
        validate_assignment = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            CaseStatus: lambda v: v.value,
            FrozenSet: lambda v: list(v),
        }
        frozen = True

    @root_validator(pre=True)
    def validate_positive_ids(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        int_fields = [
            "plaintiff_id",
            "defendant_id",
            "thread_id",
            "trial_thread_id",
            "log_message_id",
        ]
        for field in int_fields:
            value = values.get(field)
            if value is not None:
                try:
                    cast(int, value)
                except (ValueError, TypeError):
                    raise ValueError(f"{field} must be a valid integer")
        return values

    @root_validator
    def check_consistency(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        thread_id = values.get("thread_id")
        trial_thread_id = values.get("trial_thread_id")
        if thread_id is not None and trial_thread_id is not None:
            if thread_id == trial_thread_id:
                raise ValueError("thread_id and trial_thread_id must be different")

        plaintiff_id = values.get("plaintiff_id")
        defendant_id = values.get("defendant_id")
        if plaintiff_id is not None and defendant_id is not None:
            if plaintiff_id == defendant_id:
                raise ValueError("plaintiff_id and defendant_id must be different")
        return values

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        return cast(T, cls(**data))

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return super().dict(*args, **kwargs)


@runtime_checkable
class LMDB(Protocol):
    def begin(self, write: bool = ..., buffers: bool = ...) -> Any: ...
    def close(self) -> None: ...
    def open(self, path: str, **kwargs: Any) -> None: ...


class Store(Generic[T]):
    def __init__(self, config: Config) -> None:
        self.config: Final[Config] = config
        self.env: Optional[LMDB] = None
        self.locks: ContextVarLock = SortedDict()
        self.global_lock: Lock = asyncio.Lock()
        self.thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=4)
        self._open_db()

    def _open_db(self) -> None:
        try:
            db_path = os.path.join(BASE_DIR, "cases.lmdb")
            self.env = lmdb.open(
                str(db_path),
                map_size=10 * 1024 * 1024,
                max_dbs=1,
                subdir=False,
                readonly=False,
                lock=True,
                readahead=False,
                meminit=False,
                max_readers=126,
                sync=True,
                map_async=True,
                mode=0o600,
                create=True,
                writemap=True,
                metasync=False,
                max_spare_txns=2,
            )
            logging.info(f"LMDB environment successfully opened at {db_path}.")
        except lmdb.Error as e:
            logging.critical(f"Failed to open LMDB database: {e}")
            raise RuntimeError(f"Failed to open database: {e}") from e

    def close_db(self) -> None:
        if self.env:
            try:
                self.env.close()
                logging.info("LMDB environment closed successfully.")
            except lmdb.Error as e:
                logging.error(f"Error closing LMDB environment: {e}")
            finally:
                self.env = None
        self.thread_pool.shutdown(wait=True)
        logging.info("Thread pool executor shut down.")

    @asynccontextmanager
    async def db_operation(self) -> AsyncGenerator[LMDB, None]:
        async with self.global_lock:
            if self.env is None:
                self._open_db()
            try:
                yield cast(LMDB, self.env)
            except lmdb.Error as e:
                logging.error(f"Database operation failed: {e}")
                raise RuntimeError(f"Database operation failed: {e}") from e

    async def execute_db_operation(
        self,
        operation: Callable[[LMDB, Optional[str], Optional[Dict[str, Any]]], Any],
        case_id: Optional[str] = None,
        case_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        async with self.db_operation() as env:
            if case_id:
                lock = self.locks.setdefault(case_id, asyncio.Lock())
                async with lock:
                    logging.debug(f"Acquired lock for case_id: {case_id}")
                    return await asyncio.get_event_loop().run_in_executor(
                        self.thread_pool, operation, env, case_id, case_data
                    )
            else:
                logging.debug("Executing operation without case_id.")
                return await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool, operation, env, None, None
                )

    async def upsert_case(self, case_id: str, case_data: Dict[str, Any]) -> None:
        if not case_data:
            logging.error("Attempted to upsert with empty case_data.")
            raise ValueError("case_data cannot be empty for upsert operation.")
        await self.execute_db_operation(self._upsert_case_sync, case_id, case_data)
        logging.info(f"Upserted case with case_id: {case_id}.")

    async def delete_case(self, case_id: str) -> None:
        await self.execute_db_operation(self._delete_case_sync, case_id)
        logging.info(f"Deleted case with case_id: {case_id}.")

    async def get_all_cases(self) -> Dict[str, Dict[str, Any]]:
        cases = await self.execute_db_operation(self._get_all_cases_sync)
        logging.debug(f"Retrieved all cases. Total cases: {len(cases)}.")
        return cases

    async def get_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        case = await self.execute_db_operation(self._get_case_sync, case_id)
        if case:
            logging.debug(f"Retrieved case with case_id: {case_id}.")
        else:
            logging.warning(f"Case with case_id: {case_id} not found.")
        return case

    @staticmethod
    def _upsert_case_sync(
        env: LMDB, case_id: str, case_data: Optional[Dict[str, Any]]
    ) -> None:
        if case_data is None:
            raise ValueError("case_data cannot be None for upsert operation")
        with env.begin(write=True) as txn:
            txn.put(case_id.encode("utf-8"), orjson.dumps(case_data))
        logging.debug(f"Case {case_id} upserted in LMDB.")

    @staticmethod
    def _delete_case_sync(env: LMDB, case_id: str, _: Optional[Dict[str, Any]]) -> None:
        with env.begin(write=True) as txn:
            txn.delete(case_id.encode("utf-8"))
        logging.debug(f"Case {case_id} deleted from LMDB.")

    @staticmethod
    def _get_all_cases_sync(
        env: LMDB, _: Optional[str], __: Optional[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        with env.begin(buffers=True) as txn:
            cases = {
                key.decode("utf-8"): orjson.loads(value) for key, value in txn.cursor()
            }
        logging.debug("All cases retrieved from LMDB.")
        return cases

    @staticmethod
    def _get_case_sync(
        env: LMDB, case_id: Optional[str], _: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if case_id is None:
            raise ValueError("case_id cannot be None for get_case operation")
        with env.begin(buffers=True) as txn:
            case_data = txn.get(case_id.encode("utf-8"))
            if case_data:
                logging.debug(f"Case {case_id} found in LMDB.")
                return orjson.loads(case_data)
            logging.debug(f"Case {case_id} not found in LMDB.")
            return None


def async_retry(
    *,
    max_retries: int = 3,
    delay: float = 0.1,
    exceptions: tuple = (Exception,),
    backoff: float = 2.0,
    logger: Optional[logging.Logger] = None,
) -> Callable:
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        if logger:
                            logger.error(
                                f"Function `{func.__name__}` failed after {max_retries} attempts."
                            )
                        raise
                    if logger:
                        logger.warning(
                            f"Attempt {attempt} for function `{func.__name__}` failed with {e}. Retrying in {current_delay} seconds..."
                        )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff

        return wrapper

    return decorator


class Repository(Generic[T]):
    def __init__(self, config: Config, case_store: Store[T]) -> None:
        self.config: Final[Config] = config
        self.case_store: Final[Store[T]] = case_store
        self.current_cases: TTLCache[str, T] = TTLCache(maxsize=1000, ttl=3600)
        self._case_locks: SortedDict[str, asyncio.Lock] = SortedDict()
        self._global_lock: Final[asyncio.Lock] = asyncio.Lock()

    @asynccontextmanager
    async def case_lock(self, case_id: str) -> AsyncIterator[None]:
        async with self._global_lock:
            lock = self._case_locks[case_id]
        try:
            await asyncio.wait_for(lock.acquire(), timeout=5.0)
            yield
        finally:
            if lock.locked():
                lock.release()

    @async_retry(
        max_retries=5, delay=0.2, exceptions=(ValidationError, asyncio.TimeoutError)
    )
    async def get_case(self, case_id: str) -> Optional[T]:
        cached_case = self.current_cases.get(case_id)
        if cached_case:
            return cached_case

        case_data = await self.case_store.get_case(case_id)
        if case_data is None:
            logger.debug(f"Case {case_id} not found in store.")
            return None

        try:
            case = self._create_case_instance(case_data)
            self.current_cases[case_id] = case
            logger.debug(f"Case {case_id} retrieved and cached.")
            return case
        except ValidationError as e:
            logger.error(f"Validation error for case {case_id}: {e}")
            return None

    @async_retry(max_retries=5, delay=0.2, exceptions=(asyncio.TimeoutError,))
    async def persist_case(self, case_id: str, case: T) -> None:
        async with self.case_lock(case_id):
            self.current_cases[case_id] = case
            await self.case_store.upsert_case(case_id, case.dict())
            logger.debug(f"Case {case_id} persisted and cached.")

    @async_retry(max_retries=3, delay=0.5, exceptions=(asyncio.TimeoutError,))
    async def update_case(self, case_id: str, **kwargs: Any) -> None:
        async with self.case_lock(case_id):
            case = await self.get_case(case_id)
            if not case:
                logger.error(f"Case {case_id} not found for update.")
                raise KeyError(f"Case {case_id} not found.")

            updated_case = self._update_case_instance(case, **kwargs)
            await self.persist_case(case_id, updated_case)
            logger.debug(f"Case {case_id} updated with kwargs {kwargs}.")

    @async_retry(max_retries=3, delay=0.5, exceptions=(asyncio.TimeoutError,))
    async def delete_case(self, case_id: str) -> None:
        async with self.case_lock(case_id):
            try:
                await self.case_store.delete_case(case_id)
                self.current_cases.pop(case_id, None)
                logger.debug(f"Case {case_id} deleted from store and cache.")
            except Exception as e:
                logger.error(f"Error deleting case {case_id}: {e}", exc_info=True)
                raise

    @async_retry(max_retries=3, delay=0.2, exceptions=(asyncio.TimeoutError,))
    async def get_all_cases(self) -> Dict[str, T]:
        case_data = await self.case_store.get_all_cases()
        valid_cases = {}
        for case_id, data in case_data.items():
            if self._validate_case_data(case_id, data):
                try:
                    case = self._create_case_instance(data)
                    valid_cases[case_id] = case
                    self.current_cases[case_id] = case
                except ValidationError as e:
                    logger.error(f"Validation error for case {case_id}: {e}")
        logger.debug(f"Retrieved and cached {len(valid_cases)} cases.")
        return valid_cases

    def _validate_case_data(self, case_id: str, data: Dict[str, Any]) -> bool:
        try:
            self._create_case_instance(data)
            return True
        except ValidationError as e:
            logger.error(f"Invalid data for case {case_id}: {e}")
            return False

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Store[T]]:
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
        filtered_cases = {
            cid: case for cid, case in all_cases.items() if case.status == status
        }
        logger.debug(f"Filtered cases by status {status}: {len(filtered_cases)} found.")
        return filtered_cases

    async def get_cases_by_user(self, user_id: int) -> Dict[str, T]:
        all_cases = await self.get_all_cases()
        user_cases = {
            cid: case
            for cid, case in all_cases.items()
            if case.plaintiff_id == user_id or case.defendant_id == user_id
        }
        logger.debug(f"Filtered cases by user {user_id}: {len(user_cases)} found.")
        return user_cases

    async def cleanup_expired_cases(self, expiration_days: int) -> None:
        current_time = datetime.now(timezone.utc)
        expiration_threshold = current_time - timedelta(days=expiration_days)

        all_cases = await self.get_all_cases()
        expired_cases = [
            cid
            for cid, case in all_cases.items()
            if case.status == CaseStatus.CLOSED
            and case.updated_at < expiration_threshold
        ]

        tasks = [self.delete_case(cid) for cid in expired_cases]
        await asyncio.gather(*tasks)
        logger.info(f"Cleaned up {len(expired_cases)} expired cases.")


# View


class View:
    PROVERBS: Final[Tuple[str, ...]] = (
        "Iuris prudentia est divinarum atque humanarum rerum notitia, iusti atque iniusti scientia (D. 1, 1, 10, 2).",
        "Iuri operam daturum prius nosse oportet, unde nomen iuris descendat. est autem a iustitia appellatum: nam, ut eleganter celsus definit, ius est ars boni et aequi (D. 1, 1, 1, pr.).",
        "Iustitia est constans et perpetua voluntas ius suum cuique tribuendi (D. 1, 1, 10, pr.).",
    )

    def __init__(self, bot: interactions.Client, config: Config):
        self.bot: Final[interactions.Client] = bot
        self.config: Final[Config] = config
        self.embed_cache: Dict[str, interactions.Embed] = {}
        self.button_cache: Dict[str, List[interactions.Button]] = {}
        logger.debug("View initialized with embed and button caches.")

    async def create_embed(
        self, title: str, description: str = "", color: EmbedColor = EmbedColor.INFO
    ) -> interactions.Embed:
        cache_key = f"{title}:{description}:{color.value}"
        if embed := self.embed_cache.get(cache_key):
            logger.debug(f"Using cached embed for key: {cache_key}")
            return embed

        try:
            embed = interactions.Embed(
                title=title, description=description, color=color.value
            )
            guild: Optional[interactions.Guild] = await self.bot.fetch_guild(
                self.config.GUILD_ID
            )
            if guild and guild.icon:
                embed.set_footer(text=guild.name, icon_url=str(guild.icon.url))
            embed.timestamp = datetime.now(timezone.utc)
            embed.set_footer(text="鍵政大舞台")
            self.embed_cache[cache_key] = embed
            logger.debug(f"Created and cached new embed for key: {cache_key}")
            return embed
        except Exception as e:
            logger.error(f"Failed to create embed: {e}", exc_info=True)
            raise

    async def send_response(
        self,
        ctx: interactions.InteractionContext,
        title: str,
        message: str,
        color: EmbedColor,
    ) -> None:
        try:
            embed = await self.create_embed(title, message, color)
            await ctx.send(embed=embed, ephemeral=True)
            logger.debug(f"Sent {title} response to context {ctx.id}.")
        except HTTPException as e:
            logger.warning(f"Failed to send response due to HTTPException: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in send_response: {e}", exc_info=True)

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
        modal = interactions.Modal(*fields, title=title, custom_id=custom_id)
        logger.debug(f"Created modal: {custom_id} with title: {title}")
        return modal

    @staticmethod
    @lru_cache(maxsize=2)
    def define_modal_fields(
        is_appeal: bool,
    ) -> Tuple[Union[interactions.ShortText, interactions.ParagraphText], ...]:
        fields = [
            (
                interactions.ShortText(
                    custom_id="defendant_id",
                    label="Case Number" if is_appeal else "Defendant ID",
                    placeholder=(
                        "Enter the original case number"
                        if is_appeal
                        else "Enter the defendant's user ID"
                    ),
                    required=True,
                )
                if field[0] != "facts"
                else interactions.ParagraphText(
                    custom_id="facts",
                    label="Facts",
                    placeholder="Describe the relevant facts",
                    required=True,
                )
            )
            for field in [
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
                    "Enter the reason for appeal" if is_appeal else "Law article",
                ),
                ("facts", "Facts", "Describe the relevant facts"),
            ]
        ]
        logger.debug(f"Defined modal fields for is_appeal={is_appeal}")
        return tuple(fields)

    def create_action_buttons(self) -> Tuple[interactions.Button, ...]:
        buttons = (
            interactions.Button(
                style=interactions.ButtonStyle.PRIMARY,
                label="File Lawsuit",
                custom_id="initiate_lawsuit_button",
            ),
            interactions.Button(
                style=interactions.ButtonStyle.SECONDARY,
                label="File Appeal",
                custom_id="initiate_appeal_button",
            ),
        )
        logger.debug("Created action buttons for filing lawsuit and appeal.")
        return buttons

    async def create_case_summary_embed(
        self, case_id: str, case: Data
    ) -> interactions.Embed:
        fields = [
            (
                "Presiding Judge",
                " ".join(f"<@{judge_id}>" for judge_id in case.judges)
                or "Not yet assigned",
            ),
            ("Plaintiff", f"<@{case.plaintiff_id}>"),
            ("Defendant", f"<@{case.defendant_id}>"),
            ("Accusation", case.accusation or "None"),
            ("Facts", case.facts or "None"),
        ]

        embed = await self.create_embed(f"Case #{case_id}")
        for name, value in fields:
            embed.add_field(name=name, value=value, inline=False)
        logger.debug(f"Created case summary embed for case_id={case_id}")
        return embed

    def create_case_action_buttons(self, case_id: str) -> List[interactions.ActionRow]:
        buttons = [
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
        ]
        action_row = interactions.ActionRow(*buttons)
        logger.debug(f"Created action buttons for case_id={case_id}")
        return [action_row]

    def create_trial_privacy_buttons(
        self, case_id: str
    ) -> Tuple[interactions.Button, ...]:
        cache_key = f"trial_privacy_{case_id}"
        if buttons := self.button_cache.get(cache_key):
            logger.debug(f"Using cached trial privacy buttons for case_id={case_id}")
            return tuple(buttons)

        buttons = (
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
        )
        self.button_cache[cache_key] = buttons
        logger.debug(f"Created and cached trial privacy buttons for case_id={case_id}")
        return buttons

    @lru_cache(maxsize=1000)
    def create_end_trial_button(self, case_id: str) -> interactions.Button:
        button = interactions.Button(
            style=interactions.ButtonStyle.DANGER,
            label="End Trial",
            custom_id=f"end_trial_{case_id}",
        )
        logger.debug(f"Created end trial button for case_id={case_id}")
        return button

    @lru_cache(maxsize=1000)
    def create_user_management_buttons(self, user_id: int) -> Tuple[Button, ...]:
        actions = (
            ("Mute", "mute", interactions.ButtonStyle.PRIMARY),
            ("Unmute", "unmute", interactions.ButtonStyle.SUCCESS),
        )
        buttons = tuple(
            interactions.Button(
                style=style, label=label, custom_id=f"{action}_{user_id}"
            )
            for label, action, style in actions
        )
        logger.debug(f"Created user management buttons for user_id={user_id}")
        return buttons

    @lru_cache(maxsize=1000)
    def create_message_management_buttons(
        self, message_id: int
    ) -> Tuple[interactions.Button, ...]:
        buttons = (
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
        logger.debug(f"Created message management buttons for message_id={message_id}")
        return buttons

    @lru_cache(maxsize=1000)
    def create_evidence_action_buttons(
        self, case_id: str, user_id: int
    ) -> Tuple[interactions.Button, ...]:
        buttons = (
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
        logger.debug(
            f"Created evidence action buttons for case_id={case_id}, user_id={user_id}"
        )
        return buttons

    @staticmethod
    @lru_cache(maxsize=1)
    def get_proverb_selector() -> Callable[[date], str]:
        proverb_count = len(View.PROVERBS)
        shuffled_indices = tuple(random.sample(range(proverb_count), proverb_count))
        logger.debug("Initialized proverb selector with shuffled indices.")

        def select_proverb(current_date: date) -> str:
            index_position = current_date.toordinal() % proverb_count
            selected_index = shuffled_indices[index_position]
            proverb = View.PROVERBS[selected_index]
            logger.debug(f"Selected proverb for {current_date}: {proverb}")
            return proverb

        return select_proverb

    @lru_cache(maxsize=366)
    def get_daily_proverb(self, current_date: date = date.today()) -> str:
        proverb = self.get_proverb_selector()(current_date)
        logger.debug(f"Retrieved daily proverb for {current_date}: {proverb}")
        return proverb

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
        logger.info("Cleared all caches in View.")

    async def update_embed_cache(self) -> None:
        try:
            tasks = [
                self.create_embed(title, description, color)
                for title, description, color in self.embed_cache.keys()
            ]
            updated_embeds = await asyncio.gather(*tasks, return_exceptions=True)
            for key, embed in zip(self.embed_cache.keys(), updated_embeds):
                if isinstance(embed, interactions.Embed):
                    self.embed_cache[key] = embed
                    logger.debug(f"Updated embed cache for key: {key}")
                else:
                    logger.error(
                        f"Failed to update embed for key: {key}, Error: {embed}"
                    )
            logger.info("Successfully updated embed cache.")
        except Exception as e:
            logger.error(f"Error updating embed cache: {e}", exc_info=True)


async def is_judge(ctx: interactions.BaseContext) -> bool:
    return ctx.author.has_role(Config().JUDGE_ROLE_ID)


# Controller


class Lawsuit(interactions.Extension):
    ACTION_MAP: Final[Dict[str, Callable]] = {
        InteractionAction.FILE.value: lambda self, ctx, case_id: self.handle_case_action(
            ctx, InteractionAction.FILE, case_id
        ),
        InteractionAction.CLOSE.value: lambda self, ctx, case_id: self.handle_case_action(
            ctx, InteractionAction.CLOSE, case_id
        ),
        InteractionAction.WITHDRAW.value: lambda self, ctx, case_id: self.handle_case_action(
            ctx, InteractionAction.WITHDRAW, case_id
        ),
        InteractionAction.ACCEPT.value: lambda self, ctx, case_id: self.handle_case_action(
            ctx, InteractionAction.ACCEPT, case_id
        ),
        InteractionAction.DISMISS.value: lambda self, ctx, case_id: self.handle_case_action(
            ctx, InteractionAction.DISMISS, case_id
        ),
        InteractionAction.PUBLIC_TRIAL.value: lambda self, ctx: self.handle_trial_privacy(
            ctx
        ),
        InteractionAction.END_TRIAL.value: lambda self, ctx: self.handle_end_trial(ctx),
        InteractionAction.MUTE.value: lambda self, ctx: self.handle_user_message_action(
            ctx
        ),
        InteractionAction.UNMUTE.value: lambda self, ctx: self.handle_user_message_action(
            ctx
        ),
        InteractionAction.PIN.value: lambda self, ctx: self.handle_user_message_action(
            ctx
        ),
        InteractionAction.DELETE.value: lambda self, ctx: self.handle_user_message_action(
            ctx
        ),
        InteractionAction.APPROVE_EVIDENCE.value: lambda self, ctx: self.handle_judge_evidence_action(
            ctx
        ),
        InteractionAction.REJECT_EVIDENCE.value: lambda self, ctx: self.handle_judge_evidence_action(
            ctx
        ),
    }

    CASE_ACTIONS: Final[frozenset] = frozenset(
        {
            InteractionAction.FILE.value,
            InteractionAction.CLOSE.value,
            InteractionAction.WITHDRAW.value,
            InteractionAction.ACCEPT.value,
            InteractionAction.DISMISS.value,
        }
    )

    def __init__(self, bot: interactions.Client):
        self.bot: Final[interactions.Client] = bot
        self.config: Final[Config] = Config()
        self.store: Final[Store] = Store(self.config)
        self.repository: Final[Repository] = Repository(self.config, self.store)
        self.view: Final[View] = View(bot, self.config)
        self.current_cases: Final[Dict[str, Data]] = {}
        self.case_cache: Final[TTLCache[str, Data]] = TTLCache(maxsize=1000, ttl=300)
        self._case_locks: Final[SortedDict[str, asyncio.Lock]] = SortedDict()
        self._lock_timestamps: Final[SortedDict[str, float]] = SortedDict()
        self._case_lock: Final[asyncio.Lock] = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        self.case_task_queue: Final[asyncio.Queue[Callable[[], Awaitable[None]]]] = (
            asyncio.Queue()
        )
        self.shutdown_event: Final[asyncio.Event] = asyncio.Event()
        self.lawsuit_button_message_id: Optional[int] = None
        self.case_action_handlers: Final[Dict[InteractionAction, Callable]] = {
            InteractionAction.FILE: self.handle_file_case_request,
            InteractionAction.WITHDRAW: partial(
                self.handle_case_closure, action=InteractionAction.WITHDRAW
            ),
            InteractionAction.CLOSE: partial(
                self.handle_case_closure, action=InteractionAction.CLOSE
            ),
            InteractionAction.DISMISS: partial(
                self.handle_case_closure, action=InteractionAction.DISMISS
            ),
            InteractionAction.ACCEPT: self.handle_accept_case_request,
        }
        self.case_lock_cache: Final[TTLCache[str, asyncio.Lock]] = TTLCache(
            maxsize=1000, ttl=300
        )
        self._initialize_background_tasks()

    # Component functions

    @interactions.listen(NewThreadCreate)
    async def on_thread_create(self, event: NewThreadCreate) -> None:
        try:
            if event.thread.parent_id == self.config.COURTROOM_CHANNEL_ID:
                await self.delete_thread_created_message(
                    event.thread.parent_id, event.thread
                )
                logger.debug(
                    f"Deleted thread {event.thread.id} under courtroom channel."
                )
        except Exception as e:
            logger.exception(f"Failed to handle thread creation: {e}")

    @interactions.listen(MessageCreate)
    async def on_message_create(self, event: MessageCreate) -> None:
        try:
            if event.message.author.id == self.bot.user.id:
                logger.debug("Ignored message from bot itself.")
                return

            if isinstance(event.message.channel, interactions.DMChannel):
                coro = self.handle_direct_message_evidence(event.message)
                logger.debug("Handling direct message evidence.")
            else:
                coro = self.handle_channel_message(event.message)
                logger.debug("Handling channel message.")

            asyncio.create_task(coro)
            logger.debug("Created task for message handling.")
        except Exception as e:
            logger.exception(f"Error in on_message_create: {e}")

    @interactions.listen()
    async def on_component(self, component: interactions.ComponentContext) -> None:
        if not hasattr(component, "custom_id"):
            logger.warning(f"Component interaction lacks `custom_id`: {component}")
            return

        await component.defer()
        logger.debug(f"Deferred component interaction: {component.custom_id}")

        custom_id = component.custom_id
        action_match = re.match(r"^(?P<action>\w+)(?:_(?P<args>.+))?$", custom_id)
        if not action_match:
            logger.warning(f"Invalid custom_id format: {custom_id}")
            await component.send("Invalid interaction data.", ephemeral=True)
            return

        action = action_match.group("action")
        args = action_match.group("args")

        handler = self.ACTION_MAP.get(action)
        if not handler:
            logger.warning(f"No handler found for action: {action}")
            await component.send("Unhandled interaction.", ephemeral=True)
            return

        try:
            if action in self.CASE_ACTIONS:
                if args is None:
                    raise ValueError("Missing case ID for case action.")
                await handler(self, component, args)
                logger.info(f"Handled case action `{action}` for case ID `{args}`.")
            else:
                await handler(self, component, *args.split("_") if args else ())
                logger.info(f"Handled action `{action}` with args `{args}`.")
        except Exception as e:
            logger.exception(f"Error handling component `{custom_id}`: {e}")
            await component.send(
                "An error occurred while processing your request.", ephemeral=True
            )

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self) -> None:
        logger.info("Unloading Lawsuit extension.")
        self.shutdown_event.set()
        try:
            if self._cleanup_task and not self._cleanup_task.done():
                self._cleanup_task.cancel()
                await self._cleanup_task
                logger.debug("Cleanup task cancelled successfully.")
        except asyncio.TimeoutError:
            logger.error("Cleanup task timed out during extension unload.")
        except Exception as e:
            logger.exception(f"Error during extension unload: {e}")

    @interactions.component_callback("initiate_lawsuit_button")
    async def handle_lawsuit_initiation(
        self, ctx: interactions.ComponentContext
    ) -> None:
        try:
            if await self.has_ongoing_lawsuit(ctx.author.id):
                await self.view.send_error(ctx, "You already have an ongoing lawsuit.")
                logger.debug(
                    f"User {ctx.author.id} attempted to initiate multiple lawsuits."
                )
            else:
                modal = self.view.create_lawsuit_modal()
                await ctx.send_modal(modal)
                logger.info(f"Lawsuit modal sent to user {ctx.author.id}.")
        except Exception as e:
            logger.exception(f"Error in handle_lawsuit_initiation: {e}")
            await self.view.send_error(
                ctx, "An unexpected error occurred. Please try again later."
            )

    @interactions.modal_callback("lawsuit_form_modal")
    async def handle_lawsuit_form(self, ctx: interactions.ModalContext) -> None:
        try:
            is_valid, sanitized_data = self.validate_and_sanitize(ctx.responses)
            if not is_valid:
                await self.view.send_error(
                    ctx, "Invalid input. Please check your entries and try again."
                )
                logger.warning("Validation failed for lawsuit form submission.")
                return

            is_valid_defendant, defendant_id = await self.validate_defendant(
                sanitized_data
            )
            if not is_valid_defendant:
                await self.view.send_error(
                    ctx, "Invalid defendant ID. Please check and try again."
                )
                logger.warning("Defendant validation failed.")
                return

            case_id, case = await self.create_new_case(
                ctx, sanitized_data, defendant_id
            )
            if not case_id or not case:
                await self.view.send_error(
                    ctx,
                    "An error occurred while creating the case. Please try again later.",
                )
                logger.error("Failed to create new case.")
                return

            success = await self.setup_mediation_thread(case_id, case)
            if not success:
                await self.repository.delete_case(case_id)
                await self.view.send_error(
                    ctx,
                    "An error occurred while setting up the mediation thread. Please try again later.",
                )
                logger.error("Failed to set up mediation thread; case deleted.")
                return

            try:
                await self.view.send_success(
                    ctx, f"Mediation room #{case_id} created successfully."
                )
                logger.info(
                    f"Successfully sent lawsuit creation success message for case {case_id}."
                )
            except NotFound:
                logger.warning(
                    "Interaction not found when trying to send success message."
                )

            await self.notify_judges(case_id, ctx.guild_id)
            logger.info(f"Notified judges about the new case {case_id}.")

        except Exception as e:
            logger.exception(f"Error in handle_lawsuit_form: {e}")
            try:
                await self.view.send_error(
                    ctx, "An unexpected error occurred. Please try again later."
                )
            except NotFound:
                logger.warning(
                    "Interaction not found when trying to send error message."
                )

    # Task functions

    def _initialize_background_tasks(self) -> None:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self.fetch_cases(), name="FetchCasesTask"),
            loop.create_task(
                self.daily_embed_update_task(), name="DailyEmbedUpdateTask"
            ),
            loop.create_task(self.process_task_queue(), name="ProcessTaskQueue"),
            loop.create_task(
                self.periodic_consistency_check(), name="ConsistencyCheckTask"
            ),
        ]
        for task in tasks:
            task.add_done_callback(self._handle_task_exception)

    def _handle_task_exception(self, task: asyncio.Task) -> None:
        if task.cancelled():
            logger.info(f"Background task {task.get_name()} was cancelled.")
            return
        exception = task.exception()
        if exception:
            if isinstance(exception, asyncio.CancelledError):
                logger.info(f"Background task {task.get_name()} was cancelled.")
            else:
                logger.critical(
                    f"Exception in background task {task.get_name()}: {exception}",
                    exc_info=True,
                )

    async def cleanup_locks(self) -> None:
        async with self._case_lock:
            self._case_locks.clear()
            self._lock_timestamps.clear()
            logger.info("All case locks and timestamps have been cleared.")

    def __del__(self) -> None:
        if (
            hasattr(self, "_cleanup_task")
            and self._cleanup_task
            and not self._cleanup_task.done()
        ):
            asyncio.create_task(self._cleanup_task)
            logger.info("Cleanup task has been scheduled for cancellation.")

    async def fetch_cases(self) -> None:
        logger.info("Starting fetch_cases operation.")
        try:
            cases = await self.store.get_all_cases()
            self.current_cases = {
                case_id: Data(**case_data) for case_id, case_data in cases.items()
            }
            logger.info(f"Loaded {len(self.current_cases)} cases successfully.")
        except Exception as e:
            logger.exception("Error fetching cases.", exc_info=True)
            self.current_cases.clear()
            logger.info("In-memory case cache has been cleared due to fetch failure.")

    async def process_task_queue(self) -> None:
        logger.info("Starting process_task_queue.")
        while not self.shutdown_event.is_set():
            try:
                task_callable = await asyncio.wait_for(
                    self.case_task_queue.get(), timeout=1.0
                )
                asyncio.create_task(task_callable(), name="CaseTaskProcessor")
                self.case_task_queue.task_done()
                logger.debug("Processed a task from the queue.")
            except asyncio.TimeoutError:
                await asyncio.sleep(0)
            except Exception as e:
                logger.exception("Error processing task from the queue.", exc_info=True)
                self.case_task_queue.task_done()

    async def periodic_consistency_check(self) -> None:
        logger.info("Starting periodic_consistency_check.")
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)
                await self.verify_consistency()
            except asyncio.CancelledError:
                logger.info("Consistency check task was cancelled.")
                break
            except Exception as e:
                logger.exception(
                    "Error during periodic consistency check.", exc_info=True
                )

    async def daily_embed_update_task(self) -> None:
        logger.info("Starting daily_embed_update_task.")
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(86400)
                await self.update_lawsuit_button_embed()
            except asyncio.CancelledError:
                logger.info("Daily embed update task was cancelled.")
                break
            except Exception as e:
                logger.error(f"Error updating lawsuit button embed: {e}", exc_info=True)

    async def verify_consistency(self) -> None:
        logger.info("Starting data consistency check.")
        try:
            storage_cases = await self.store.get_all_cases()
            storage_case_ids = set(storage_cases.keys())
            current_case_ids = set(self.current_cases.keys())

            missing_in_storage = current_case_ids - storage_case_ids
            missing_in_memory = storage_case_ids - current_case_ids
            inconsistent = {
                case_id
                for case_id in current_case_ids & storage_case_ids
                if self.current_cases[case_id].dict() != storage_cases[case_id]
            }

            update_tasks = [
                self.repository.persist_case(case_id, self.current_cases[case_id])
                for case_id in missing_in_storage | inconsistent
            ]

            for case_id in missing_in_memory:
                self.current_cases[case_id] = Data(**storage_cases[case_id])

            if update_tasks:
                await asyncio.gather(*update_tasks)
                logger.info(f"Persisted {len(update_tasks)} cases to storage.")

            logger.info("Data consistency check completed successfully.")
        except Exception as e:
            logger.error(f"Error during consistency verification: {e}", exc_info=True)

    async def update_lawsuit_button_embed(self) -> None:
        logger.info("Updating lawsuit button embed.")
        try:
            if not self.lawsuit_button_message_id:
                logger.warning("Lawsuit button message ID is not set.")
                return

            channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            message = await channel.fetch_message(self.lawsuit_button_message_id)
            daily_proverb = self.view.get_daily_proverb()
            embed = await self.view.create_embed(
                daily_proverb, "Click the button to file a lawsuit or appeal."
            )
            await message.edit(embeds=[embed])
            logger.info("Lawsuit button embed updated successfully.")
        except interactions.NotFound:
            logger.error("Lawsuit button message not found.")
        except interactions.HTTPException as e:
            logger.exception(f"HTTP error while updating embed: {e}", exc_info=True)
        except Exception as e:
            logger.exception(f"Unexpected error updating embed: {e}", exc_info=True)

    # Command functions

    module_base: Final[interactions.SlashCommand] = interactions.SlashCommand(
        name="lawsuit", description="Lawsuit management system"
    )

    @module_base.subcommand(
        "memory", sub_cmd_description="Print top memory allocations"
    )
    @interactions.check(is_judge)
    async def display_memory_allocations(self, ctx: interactions.SlashContext) -> None:
        try:
            tracemalloc.start()
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics("lineno")[:10]

            stats_content = "\n".join(
                f"{stat.count} allocations, {stat.size / 1024:.1f} KiB"
                f" in {stat.traceback.format()}"
                for stat in top_stats
            )
            content = f"```py\n{'-' * 30}\n{stats_content}\n```"

            await self.view.send_success(
                ctx, content, title="Top 10 Memory Allocations"
            )
            tracemalloc.stop()
            logger.debug("Memory allocations displayed successfully.")
        except Exception as e:
            logger.exception("Failed to display memory allocations.", exc_info=True)
            await self.view.send_error(ctx, "Failed to retrieve memory allocations.")

    @module_base.subcommand(
        "consistency", sub_cmd_description="Perform a data consistency check"
    )
    @interactions.check(is_judge)
    async def verify_consistency_command(
        self, ctx: Optional[interactions.SlashContext] = None
    ) -> None:
        try:
            if ctx:
                await ctx.defer()

            differences = await self.verify_consistency()
            formatted_results = self.format_consistency_results(differences)

            if ctx:
                await self.view.send_success(
                    ctx,
                    f"```py\n{formatted_results}\n```",
                    title="Data Consistency Check Results",
                )
            else:
                logger.info(
                    f"Automated consistency check results:\n{formatted_results}"
                )
        except Exception as e:
            logger.exception("Consistency check failed.", exc_info=True)
            if ctx:
                await self.view.send_error(ctx, "Failed to perform consistency check.")

    @staticmethod
    def format_consistency_results(differences: Dict[str, Set[str]]) -> str:
        try:
            return (
                "\n".join(
                    f"{len(cases)} cases {status}: {', '.join(sorted(cases))}"
                    for status, cases in differences.items()
                    if cases
                )
                or "No inconsistencies found."
            )
        except Exception as e:
            logger.exception("Error formatting consistency results.", exc_info=True)
            return "Error formatting consistency results."

    @module_base.subcommand(
        "dispatch",
        sub_cmd_description="Dispatch the lawsuit button to the specified channel",
    )
    @interactions.check(is_judge)
    async def dispatch_lawsuit_button(self, ctx: interactions.SlashContext) -> None:
        try:
            components = self.view.create_action_buttons()
            daily_proverb = self.view.get_daily_proverb()
            embed = await self.view.create_embed(
                daily_proverb, "Click the button to file a lawsuit or appeal."
            )

            action_row = interactions.ActionRow(*components)

            message = await ctx.channel.send(embeds=[embed], components=[action_row])

            self.lawsuit_button_message_id = message.id
            await self.initialize_chat_thread(ctx)
            logger.info(f"Lawsuit button dispatched by user {ctx.author.id}.")
        except HTTPException as e:
            if e.status == 404:
                logger.warning("Interaction not found, possibly due to timeout.")
            else:
                logger.error(f"Error in dispatch_lawsuit_button: {e}", exc_info=True)
                await self.view.send_error(ctx, "Failed to dispatch lawsuit button.")
        except Exception as e:
            logger.exception(
                "Unexpected error in dispatch_lawsuit_button.", exc_info=True
            )
            await self.view.send_error(ctx, "An unexpected error occurred.")

    @module_base.subcommand(
        "role", sub_cmd_description="Assign or revoke a role for a user in a case"
    )
    @interactions.slash_option(
        name="action",
        description="Action to perform",
        opt_type=interactions.OptionType.STRING,
        required=True,
        choices=[
            interactions.SlashCommandChoice(
                name="Assign", value=InteractionAction.ASSIGN.value
            ),
            interactions.SlashCommandChoice(
                name="Revoke", value=InteractionAction.REVOKE.value
            ),
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
        try:
            case_id = await self.find_case_number_by_channel_id(ctx.channel_id)
            if not case_id:
                await self.view.send_error(ctx, "Unable to find the associated case.")
                logger.warning(f"Case ID not found for channel {ctx.channel_id}.")
                return

            interaction_action = InteractionAction(action.lower())
            if interaction_action not in InteractionAction:
                await self.view.send_error(ctx, "Invalid action specified.")
                logger.warning(f"Invalid action `{action}` received.")
                return

            await self.manage_case_role(ctx, case_id, interaction_action, role, user)
            logger.info(
                f"Role `{role}` {'assigned' if action == 'assign' else 'revoked'} for user {user.id} in case {case_id}."
            )
        except Exception as e:
            logger.exception("Error managing role.", exc_info=True)
            await self.view.send_error(
                ctx, "An error occurred while managing the role."
            )

    # Serve functions

    async def handle_case_action(
        self,
        ctx: interactions.ComponentContext,
        action: InteractionAction,
        case_id: str,
    ) -> None:
        logger.info(f"Handling case action: {action} for case {case_id}")

        async with AsyncExitStack() as stack:
            try:
                case = await stack.enter_async_context(self.case_context(case_id))
                if case is None:
                    await self.view.send_error(ctx, "Case not found.")
                    return

                guild = await self.bot.fetch_guild(self.config.GUILD_ID)
                member = await guild.fetch_member(ctx.author.id)

                handler = self.case_action_handlers.get(action)
                if handler:
                    await handler(ctx, case_id, case, member)
                else:
                    await self.view.send_error(ctx, "Invalid action specified.")

            except asyncio.TimeoutError:
                logger.error(f"Timeout acquiring lock for case {case_id}")
                await self.view.send_error(
                    ctx, "Operation timed out. Please try again."
                )
            except Exception as e:
                logger.exception(f"Error in handle_case_action: {e!r}")
                await self.view.send_error(
                    ctx, "An unexpected error occurred while processing your request."
                )

    async def handle_trial_privacy(self, ctx: interactions.ComponentContext) -> None:
        custom_id_parts = ctx.custom_id.split("_")
        if not (
            len(custom_id_parts) == 4
            and custom_id_parts[0] == "public"
            and custom_id_parts[1] == "trial"
        ):
            await self.view.send_error(ctx, "Invalid interaction data.")
            return

        is_public = custom_id_parts[2] == "yes"
        case_id = custom_id_parts[3]

        async with self.case_context(case_id) as case:
            if case is None:
                await self.view.send_error(ctx, "Case not found.")
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

        if await self.user_has_judge_role(user):
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
        action: InteractionAction,
    ) -> None:
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
        notification = f"第{case_id}号案已被{ctx.author.display_name}{action_name}。"
        await asyncio.gather(
            self.notify_case_participants(case_id, notification),
            self.view.send_success(ctx, f"第{case_id}号案成功{action_name}。"),
        )

    async def handle_accept_case_request(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        case: Data,
        member: interactions.Member,
    ) -> None:
        if not await self.is_user_assigned_to_case(ctx.author.id, case_id, "judges"):
            await self.view.send_error(ctx, "Insufficient permissions.")
            return

        embed = await self.view.create_embed("Public or Private Trial")
        buttons = self.view.create_trial_privacy_buttons(case_id)
        await ctx.send(embeds=[embed], components=[interactions.ActionRow(*buttons)])

    async def handle_end_trial(self, ctx: interactions.ComponentContext) -> None:
        if match := re.match(r"^end_trial_([a-f0-9]{12})$", ctx.custom_id):
            case_id = match.group(1)
            await self.handle_case_action(ctx, InteractionAction.CLOSE, case_id)
        else:
            await self.view.send_error(ctx, "Invalid interaction data.")

    # Add members to thread

    async def add_members_to_thread(
        self, thread: interactions.GuildText, member_ids: Set[int]
    ) -> None:
        async def _add_member_to_thread(member_id: int) -> None:
            try:
                user = await thread.guild.fetch_member(member_id)
                await thread.add_member(user)
            except Exception as e:
                logger.error(
                    f"Failed to add user {member_id} to thread: {e!r}", exc_info=True
                )

        await asyncio.gather(
            *(_add_member_to_thread(member_id) for member_id in member_ids),
            return_exceptions=True,
        )

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
                    raise ValueError(
                        f"This case already has the maximum number of judges ({self.config.MAX_JUDGES_PER_CASE})."
                    )

                if ctx.author.id not in case.judges:
                    new_judges = (*case.judges, ctx.author.id)
                    await self.repository.update_case(case_id, judges=new_judges)

            notification = (
                f"{ctx.author.display_name} has joined Case #{case_id} as a judge."
            )

            await asyncio.gather(
                self.view.send_success(
                    ctx, f"You have joined Case #{case_id} as a judge."
                ),
                self.notify_case_participants(case_id, notification),
            )
        except ValueError as e:
            await self.view.send_error(ctx, str(e))
        except Exception as e:
            logger.exception(f"Error in add_judge_to_case for case {case_id}: {e!r}")
            await self.view.send_error(
                ctx, "An unexpected error occurred while processing your request."
            )

    # User actions

    async def handle_user_message_action(
        self, ctx: interactions.ComponentContext
    ) -> None:
        try:
            action, target_id = self._parse_custom_id(ctx.custom_id)
            case_id = await self._validate_user_permissions(ctx)
            await self._perform_user_action(ctx, case_id, target_id, action)
        except (ValueError, PermissionError) as e:
            await self.view.send_error(ctx, str(e))
        except Exception as e:
            logger.exception(f"Unexpected error in handle_user_message_action: {e}")
            await self.view.send_error(ctx, "An unexpected error occurred.")

    @staticmethod
    def _parse_custom_id(custom_id: str) -> Tuple[InteractionAction, int]:
        if match := re.match(r"^(mute|unmute|pin|delete)_(\d{1,20})$", custom_id):
            action, target_id = match.groups()
            return InteractionAction[action.upper()], int(target_id)
        raise ValueError(f"Invalid custom_id format: {custom_id}")

    async def _validate_user_permissions(
        self, ctx: interactions.ComponentContext
    ) -> str:
        case_id = self.find_case_number_by_channel_id(ctx.channel_id)
        if not case_id or not await self.is_user_assigned_to_case(
            ctx.author.id, case_id, "judges"
        ):
            raise PermissionError("Insufficient permissions.")
        return case_id

    async def _perform_user_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        action: InteractionAction,
    ) -> None:
        action_handlers = {
            InteractionAction.MUTE: self._handle_mute_action,
            InteractionAction.UNMUTE: self._handle_mute_action,
            InteractionAction.PIN: self._handle_message_action,
            InteractionAction.DELETE: self._handle_message_action,
        }
        handler = action_handlers.get(action)
        if not handler:
            raise ValueError(f"Invalid action: {action}")

        await handler(
            ctx,
            case_id,
            target_id,
            mute=(
                (action == InteractionAction.MUTE)
                if action in {InteractionAction.MUTE, InteractionAction.UNMUTE}
                else None
            ),
            action_type=(
                action.name.lower()
                if action in {InteractionAction.PIN, InteractionAction.DELETE}
                else None
            ),
        )

    async def _handle_mute_action(
        self,
        ctx: interactions.ComponentContext,
        case_id: str,
        target_id: int,
        *,
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
        *,
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
                ctx, f"Insufficient permissions to {action_type} messages."
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
            participants = frozenset(
                {case.plaintiff_id, case.defendant_id, *case.judges}
            )
            embed = await self.view.create_embed(f"Case {case_id} Update", message)
            await asyncio.gather(
                *(
                    self._send_dm_with_fallback(participant_id, embed=embed)
                    for participant_id in participants
                ),
                return_exceptions=True,
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
            frozenset(case.judges),
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

        judge_members = frozenset(
            member for member in guild.members if judge_role in member.roles
        )

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
                label="Join Mediation",
            )
            notification_embed = await self.view.create_embed(
                f"Case #{case_id} Complaint Received",
                f"Please proceed to the [mediation room]({thread_link}) to participate in mediation.",
            )

            await self._notify_users(
                frozenset(member.id for member in judge_members),
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
            logger.warning(f"Failed to send DM to user {user_id}: {e!r}", exc_info=True)
        except Exception as e:
            logger.error(
                f"Unexpected error sending DM to user {user_id}: {e!r}", exc_info=True
            )

    async def _notify_users(
        self,
        user_ids: Iterable[int],
        notification_func: Callable[[int], Awaitable[None]],
    ) -> None:
        async def notify(user_id: int) -> None:
            try:
                await asyncio.wait_for(notification_func(user_id), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"Notification timed out for user {user_id}")
            except HTTPException as e:
                log_level = logging.WARNING if e.status == 403 else logging.ERROR
                logger.log(
                    log_level,
                    f"HTTP error notifying user {user_id}: {e!r}",
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error notifying user {user_id}: {e!r}", exc_info=True
                )

        await asyncio.gather(*(notify(user_id) for user_id in set(user_ids)))

    # Role functions

    @manage_role.autocomplete("role")
    async def role_autocomplete(
        self, ctx: interactions.AutocompleteContext
    ) -> List[interactions.Choice]:
        focused_option = ctx.focused.lower()
        return [
            interactions.Choice(name=role.value, value=role.name)
            for role in CaseRole
            if focused_option in role.value.lower()
        ][:25]

    async def manage_case_role(
        self,
        ctx: interactions.SlashContext,
        case_id: str,
        action: InteractionAction,
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

                match action:
                    case InteractionAction.ASSIGN:
                        if user.id in role_users:
                            raise ValueError(
                                f"User already has the role {case_role.value}"
                            )
                        role_users.add(user.id)
                        action_str = "Assigned"
                    case InteractionAction.REVOKE:
                        if user.id not in role_users:
                            raise ValueError(
                                f"User doesn't have the role {case_role.value}"
                            )
                        role_users.remove(user.id)
                        action_str = "Revoked"
                    case _:
                        raise ValueError(f"Invalid action: {action}")

                await self.repository.update_case(case_id, roles=roles)

                notification_message = (
                    f"<@{ctx.author.id}> has {action_str.lower()} the role of {case_role.value} "
                    f"{'to' if action == InteractionAction.ASSIGN else 'from'} <@{user.id}>."
                )

                await asyncio.gather(
                    self.view.send_success(
                        ctx,
                        f"{action_str} role {case_role.value} "
                        f"{'to' if action == InteractionAction.ASSIGN else 'from'} <@{user.id}>",
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
                logger.exception(f"Error in manage_case_role for case {case_id}: {e}")
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
                    logger.warning(f"Failed to fetch attachment: {response.status}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching attachment: {attachment['url']}")
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching attachment: {str(e)}")
        return None

    async def handle_judge_evidence_action(
        self, ctx: interactions.ComponentContext
    ) -> None:
        if match := re.match(
            r"(approve|reject)_evidence_([a-f0-9]{12})_(\d+)", ctx.custom_id
        ):
            action, case_id, user_id = match.groups()
            await self.process_judge_evidence_decision(
                ctx, action, case_id, int(user_id)
            )
        else:
            logger.warning(f"Invalid custom_id format: {ctx.custom_id}")
            await self.view.send_error(ctx, "Invalid evidence action.")

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

        files = [
            file
            for file in await asyncio.gather(
                *(
                    self.fetch_attachment_as_file(attachment)
                    for attachment in evidence.attachments
                )
            )
            if file
        ]

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
                "Your submitted evidence has been rejected and will not be made public."
            ),
            self.view.send_success(
                ctx, "The evidence has been rejected and will not be made public."
            ),
        )

    async def handle_direct_message_evidence(
        self, message: interactions.Message
    ) -> None:
        try:
            case_id = await self.get_user_active_case_number(message.author.id)
            if case_id is None:
                await message.author.send(
                    "You don't have any active cases at the moment. Evidence submission is not possible."
                )
                return

            evidence = self.create_evidence_dict(message, self.config.MAX_FILE_SIZE)
            await asyncio.gather(
                self.process_evidence(case_id, evidence),
                message.author.send("Evidence received and processed successfully."),
            )
        except HTTPException as e:
            log_message = (
                f"Cannot send DM to user {message.author.id}. They may have DMs disabled."
                if e.status == 403
                else f"HTTP error when sending DM to user {message.author.id}: {e!r}"
            )
            logger.log(
                logging.WARNING if e.status == 403 else logging.ERROR,
                log_message,
                exc_info=True,
            )

    async def handle_channel_message(self, message: interactions.Message) -> None:
        if case_id := self.find_case_number_by_channel_id(message.channel.id):
            case = await self.repository.get_case(case_id)
            if case and message.author.id in case.mute_list:
                try:
                    await message.delete()
                except Forbidden:
                    logger.warning(
                        f"No permission to delete messages in channel {message.channel.id}.",
                        exc_info=True,
                    )
                except HTTPException as e:
                    logger.error(
                        f"HTTP error when deleting message: {e!r}", exc_info=True
                    )

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
                        self.notify_judges_of_new_evidence(case_id, evidence.user_id),
                        timeout=5,
                    ),
                )

            except TimeoutError:
                logger.warning(
                    f"Notification timeout for case {case_id}", exc_info=True
                )
            except Exception as e:
                logger.critical(
                    f"Unexpected error processing evidence for case {case_id}: {e!r}",
                    exc_info=True,
                )
                raise

    # Check functions

    async def user_has_judge_role(
        self, user: Union[interactions.Member, interactions.User]
    ) -> bool:
        member: interactions.Member = (
            user
            if isinstance(user, interactions.Member)
            else await self._fetch_member_from_user(user)
        )
        return self.config.JUDGE_ROLE_ID in frozenset(member.role_ids)

    async def _fetch_member_from_user(
        self, user: interactions.User
    ) -> interactions.Member:
        guild: interactions.Guild = await self.bot.fetch_guild(self.config.GUILD_ID)
        return await guild.fetch_member(user.id)

    async def user_has_permission_for_closure(
        self, user_id: int, case_id: str, member: interactions.Member
    ) -> bool:
        return await asyncio.gather(
            self.is_user_assigned_to_case(user_id, case_id, "plaintiff_id"),
            asyncio.to_thread(lambda: self.config.JUDGE_ROLE_ID in member.role_ids),
        ) == [True, True]

    def is_valid_attachment(
        self, attachment: interactions.Attachment, max_size: int
    ) -> bool:
        return all(
            [
                attachment.content_type in self.config.ALLOWED_MIME_TYPES,
                attachment.size <= max_size,
            ]
        )

    @lru_cache(maxsize=1024, typed=True)
    def get_role_check(self, role: str) -> Callable[[Data, int], bool]:
        role_checks: Dict[str, Callable[[Data, int], bool]] = {
            "plaintiff_id": lambda case, user_id: case.plaintiff_id == user_id,
            "defendant_id": lambda case, user_id: case.defendant_id == user_id,
            "judges": lambda case, user_id: user_id in frozenset(case.judges),
        }
        return role_checks.get(
            role, lambda case, user_id: user_id in frozenset(getattr(case, role, ()))
        )

    async def is_user_assigned_to_case(
        self, user_id: int, case_id: str, role: str
    ) -> bool:
        case: Optional[Data] = await self.repository.get_case(case_id)
        return case is not None and await asyncio.to_thread(
            self.get_role_check(role), case, user_id
        )

    async def has_ongoing_lawsuit(self, user_id: int) -> bool:
        return any(
            await asyncio.gather(
                *[
                    asyncio.to_thread(
                        lambda: case.plaintiff_id == user_id
                        and case.status != CaseStatus.CLOSED
                    )
                    for case in self.current_cases.values()
                ]
            )
        )

    def validate_and_sanitize(
        self, responses: Dict[str, str]
    ) -> Tuple[bool, Dict[str, str]]:
        sanitized_data: Dict[str, str] = {
            key: self.sanitize_user_input(value) for key, value in responses.items()
        }
        return len(sanitized_data) == 3 and all(sanitized_data.values()), sanitized_data

    async def validate_defendant(
        self, data: Dict[str, str]
    ) -> Tuple[bool, Optional[int]]:
        try:
            defendant_id: int = int(data["defendant_id"])
            await self.bot.fetch_user(defendant_id)
            return True, defendant_id
        except (ValueError, NotFound) as e:
            logger.error(f"Invalid defendant ID: {data['defendant_id']}. Error: {e!r}")
            return False, None

    # Utility functions

    async def archive_thread(self, thread_id: int) -> bool:
        try:
            thread = await self.bot.fetch_channel(thread_id)
            if not isinstance(thread, interactions.ThreadChannel):
                logger.error(f"Channel ID {thread_id} is not a thread channel.")
                return False
            await thread.edit(archived=True, locked=True)
            logger.info(f"Successfully archived and locked thread {thread_id}.")
            return True
        except HTTPException as e:
            logger.error(f"HTTP error while archiving thread {thread_id}: {e}")
        except Exception as e:
            logger.error(f"Failed to archive thread {thread_id}: {e}", exc_info=True)
        return False

    @staticmethod
    def sanitize_user_input(text: str, max_length: int = 2000) -> str:

        ALLOWED_TAGS: Final[frozenset] = frozenset()
        ALLOWED_ATTRIBUTES: Final[Dict[str, frozenset]] = {}
        WHITESPACE_PATTERN: Final = re.compile(r"\s+")

        cleaner = functools.partial(
            bleach.clean,
            tags=ALLOWED_TAGS,
            attributes=ALLOWED_ATTRIBUTES,
            strip=True,
            protocols=bleach.ALLOWED_PROTOCOLS,
        )

        sanitized_text = WHITESPACE_PATTERN.sub(" ", cleaner(text)).strip()
        return sanitized_text[:max_length]

    async def delete_thread_created_message(
        self, channel_id: int, thread: interactions.ThreadChannel
    ) -> bool:
        try:
            parent_channel = await self.bot.fetch_channel(channel_id)
            if not isinstance(parent_channel, interactions.GuildText):
                logger.error(f"Channel ID {channel_id} is not a GuildText channel.")
                return False

            async for message in parent_channel.history(limit=50):
                if (
                    message.type == interactions.MessageType.THREAD_CREATED
                    and message.thread_id == thread.id
                ):
                    await message.delete()
                    logger.info(
                        f"Deleted thread creation message for thread ID: {thread.id}"
                    )
                    return True
            logger.warning(
                f"No thread creation message found for thread ID: {thread.id} in channel {channel_id}."
            )
        except HTTPException as e:
            logger.warning(
                f"HTTP error while deleting thread creation message for thread ID {thread.id}: {e}"
            )
        except Forbidden as e:
            logger.warning(
                f"Forbidden to delete messages in channel {channel_id} for thread ID {thread.id}: {e}"
            )
        except Exception as e:
            logger.error(
                f"Unexpected error deleting thread creation message: {e}", exc_info=True
            )
        return False

    async def initialize_chat_thread(self, ctx: interactions.CommandContext) -> bool:
        try:
            channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            if not isinstance(channel, interactions.GuildText):
                logger.error("Invalid courtroom channel type.")
                return False

            thread = await self.get_or_create_chat_thread(channel)
            embed = await self.create_chat_room_setup_embed(thread)

            try:
                await self.view.send_success(ctx, embed.description)
                logger.info("Chat thread initialized successfully.")
            except NotFound:
                logger.warning("Interaction not found, possibly due to timeout.")
                try:
                    await ctx.send(embed=embed, ephemeral=True)
                    logger.info("Fallback embed sent successfully.")
                except Forbidden:
                    logger.error("Forbidden to send fallback embed.")
                except Exception as e:
                    logger.error(
                        f"Failed to send follow-up message: {e}", exc_info=True
                    )
            return True

        except HTTPException as e:
            logger.error(f"HTTP error during chat thread initialization: {e}")
        except Exception as e:
            logger.error(f"Chat thread initialization error: {e}", exc_info=True)
            try:
                await self.view.send_error(
                    ctx,
                    "An error occurred while setting up the chat thread. Please try again later.",
                )
                logger.info("Sent error message to user.")
            except interactions.NotFound:
                logger.warning(
                    "Interaction not found when trying to send error message."
                )
        return False

    async def setup_mediation_thread(self, case_id: str, case: Data) -> bool:
        try:
            channel = await self.get_courtroom_channel()
            thread = await self.create_mediation_thread(channel, case_id)
            await self.send_case_summary(thread, case_id, case)
            await self.update_case_with_thread(case_id, thread.id)

            guild = await self.bot.fetch_guild(self.config.GUILD_ID)
            if not guild:
                logger.error(f"Guild ID {self.config.GUILD_ID} not found.")
                return False

            judges = await self.get_judges(guild)
            participants = {case.plaintiff_id, case.defendant_id, *judges}

            await self.add_members_to_thread(thread, participants)
            logger.info(f"Mediation thread setup completed for case {case_id}.")
            return True
        except HTTPException as e:
            logger.error(
                f"HTTP error setting up mediation thread for case {case_id}: {e}"
            )
        except Exception as e:
            logger.error(
                f"Failed to create mediation thread for case {case_id}: {e}",
                exc_info=True,
            )
        return False

    async def send_case_summary(
        self, thread: interactions.GuildText, case_id: str, case: Data
    ) -> bool:
        try:
            embed = await self.view.create_case_summary_embed(case_id, case)
            components = self.view.create_case_action_buttons(case_id)
            await thread.send(embeds=[embed], components=components)
            logger.info(f"Case summary sent to thread {thread.id} for case {case_id}.")
            return True
        except HTTPException as e:
            logger.error(f"HTTP error sending case summary to thread {thread.id}: {e}")
        except Exception as e:
            logger.error(
                f"Failed to send case summary to thread {thread.id}: {e}", exc_info=True
            )
        return False

    async def update_case_with_thread(self, case_id: str, thread_id: int) -> bool:
        try:
            await self.repository.update_case(case_id, thread_id=thread_id)
            logger.info(f"Case {case_id} updated with thread ID {thread_id}.")
            return True
        except HTTPException as e:
            logger.error(
                f"HTTP error updating case {case_id} with thread ID {thread_id}: {e}"
            )
        except Exception as e:
            logger.error(
                f"Failed to update case {case_id} with thread ID {thread_id}: {e}",
                exc_info=True,
            )
        return False

    def _sanitize_input(self, text: str, max_length: int = 2000) -> str:
        return text.strip()[:max_length]

    # Create functions

    async def create_mediation_thread(
        self, channel: interactions.GuildText, case_id: str
    ) -> interactions.GuildText:
        try:
            thread_name = f"第{case_id}号调解室"
            thread = await channel.create_thread(
                name=thread_name,
                thread_type=interactions.ChannelType.GUILD_PRIVATE_THREAD,
                auto_archive_duration=1440,
            )
            logger.info(
                f"Mediation thread `{thread_name}` created with ID: {thread.id}"
            )
            return thread
        except HTTPException as e:
            logger.error(f"Failed to create mediation thread `{thread_name}`: {e}")
            raise

    @staticmethod
    @lru_cache(maxsize=1024, typed=True)
    def create_case_id(length: int = 6) -> str:
        if length <= 0:
            raise ValueError("Length must be a positive integer.")
        return secrets.token_hex(length // 2)

    async def create_new_case(
        self, ctx: interactions.ModalContext, data: Dict[str, str], defendant_id: int
    ) -> Tuple[Optional[str], Optional[Data]]:
        try:
            case_id = self.create_case_id()
            case = self.create_case_data(ctx, data, defendant_id)
            await self.repository.persist_case(case_id, case)
            logger.info(f"New case created with ID: {case_id}")
            return case_id, case
        except Exception as e:
            logger.error(f"Error creating new case: {str(e)}", exc_info=True)
            return None, None

    def create_case_data(
        self, ctx: interactions.ModalContext, data: Dict[str, str], defendant_id: int
    ) -> Data:
        return Data(
            plaintiff_id=ctx.author.id,
            defendant_id=defendant_id,
            accusation=self._sanitize_input(data.get("accusation", "")),
            facts=self._sanitize_input(data.get("facts", "")),
            status=CaseStatus.FILED,
            judges=frozenset(),
        )

    async def create_new_chat_thread(
        self, channel: interactions.GuildText
    ) -> interactions.ThreadChannel:
        thread_name = "聊天室"
        try:
            thread = await channel.create_thread(
                name=thread_name,
                thread_type=interactions.ChannelType.GUILD_PUBLIC_THREAD,
                auto_archive_duration=10080,
            )
            logger.info(f"New chat thread `{thread.name}` created with ID: {thread.id}")
            return thread
        except HTTPException as e:
            logger.error(f"Failed to create chat thread `{thread_name}`: {e}")
            raise

    async def create_chat_room_setup_embed(
        self, thread: interactions.ThreadChannel
    ) -> interactions.Embed:
        guild_id = thread.guild.id
        channel_id = thread.parent_id
        thread_id = thread.id
        jump_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{thread_id}"
        try:
            embed = await self.view.send_success(
                f"The lawsuit and appeal buttons have been sent to this channel. The chat room thread is ready: [Jump to Thread]({jump_url})",
            )
            logger.debug(f"Chat room setup embed created for thread ID: {thread.id}")
            return embed
        except Exception as e:
            logger.error(f"Failed to create chat room setup embed: {e}", exc_info=True)
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
        visibility = "publicly" if is_public else "privately"
        thread_name = (
            f"Case #{case_id} {'Public' if is_public else 'Private'} Trial Chamber"
        )
        try:
            courtroom_channel = await self.bot.fetch_channel(
                self.config.COURTROOM_CHANNEL_ID
            )
            trial_thread = await courtroom_channel.create_thread(
                name=thread_name,
                thread_type=thread_type,
                auto_archive_duration=10080,
            )
            logger.info(
                f"Trial thread `{thread_name}` created with ID: {trial_thread.id}"
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
                    ctx,
                    f"Case #{case_id} will be tried {visibility}.",
                ),
                self.notify_case_participants(
                    case_id,
                    f"Case #{case_id} will be tried {visibility}.",
                ),
                self.add_members_to_thread(trial_thread, case.judges),
            )
            logger.debug(f"Trial thread setup completed for case ID: {case_id}")
        except HTTPException as e:
            logger.error(f"Failed to create trial thread {case_id}: {e}")
            await self.view.send_error(
                ctx, f"Failed to create trial thread for case {case_id}."
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error in creating trial thread for case {case_id}: {e}",
                exc_info=True,
            )
            await self.view.send_error(
                ctx,
                "An unexpected error occurred while setting up the trial thread.",
            )
            raise

    # Get functions

    async def get_allowed_roles(self, guild: interactions.Guild) -> List[int]:
        allowed_role_names = [
            "Prosecutor",
            "Private Prosecutor",
            "Plaintiff",
            "Defendant",
            "Defense Attorney",
            "Legal Representative",
            "Victim",
            "Witness",
        ]
        allowed_roles = [
            role.id for role in guild.roles if role.name in allowed_role_names
        ]
        logger.debug(f"Allowed roles fetched: {allowed_roles}")
        return allowed_roles

    @lru_cache(maxsize=128)
    def get_action_handler(self, action: InteractionAction) -> Callable:
        action_map = {
            InteractionAction.MUTE: self._handle_mute_action,
            InteractionAction.UNMUTE: self._handle_mute_action,
            InteractionAction.PIN: self._handle_message_action,
            InteractionAction.DELETE: self._handle_message_action,
        }
        handler = action_map.get(action, self._default_action_handler)
        logger.debug(f"Using handler for action {action}: {handler}")
        return handler

    def _default_action_handler(self, *args, **kwargs) -> None:
        logger.warning(f"No handler found for action {args[0]}. Ignoring.")

    @lru_cache(maxsize=1)
    async def get_judges(self, guild: interactions.Guild) -> Set[int]:
        judge_role = guild.get_role(self.config.JUDGE_ROLE_ID)
        if not judge_role:
            logger.warning(f"Judge role with ID {self.config.JUDGE_ROLE_ID} not found.")
            return set()

        judges = {
            member.id
            for member in await guild.fetch_members(limit=None).flatten()
            if self.config.JUDGE_ROLE_ID in member.role_ids
        }
        logger.debug(f"Judges fetched: {judges}")
        return judges

    async def get_courtroom_channel(self) -> interactions.GuildText:
        channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
        if not isinstance(channel, interactions.GuildText):
            error_msg = f"Invalid channel type: expected GuildText, got {type(channel)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        logger.debug(f"Courtroom channel fetched: {channel.id}")
        return channel

    @classmethod
    @lru_cache(maxsize=128, typed=True)
    def get_sanitizer(cls, max_length: int = 2000) -> Callable[[str], str]:
        sanitizer = partial(cls.sanitize_user_input, max_length=max_length)
        logger.debug(f"Sanitizer created: {sanitizer}")
        return sanitizer

    async def get_or_create_chat_thread(
        self, channel: interactions.GuildText
    ) -> interactions.ThreadChannel:
        existing_thread = await self.find_existing_chat_thread(channel)
        if existing_thread:
            logger.debug(f"Existing chat thread found: {existing_thread.id}")
            return existing_thread

        thread = await self.create_new_chat_thread(channel)
        logger.debug(f"New chat thread created: {thread.id}")
        return thread

    async def find_existing_chat_thread(
        self, channel: interactions.GuildText
    ) -> Optional[interactions.ThreadChannel]:
        active_threads = await channel.fetch_active_threads()
        for thread in active_threads.threads:
            if thread.name == "聊天室":
                logger.debug(f"Chat thread '{thread.name}' found with ID: {thread.id}")
                return thread
        logger.debug("No existing chat thread named `聊天室` found.")
        return None

    @cached(cache=TTLCache(maxsize=1024, ttl=60))
    async def get_case(self, case_id: str) -> Optional[Data]:
        case = await self.repository.get_case(case_id)
        if case:
            logger.debug(f"Case `{case_id}` retrieved from repository.")
        else:
            logger.debug(f"Case `{case_id}` not found in repository.")
        return case

    @lru_cache(maxsize=128)
    async def get_user_active_case_number(self, user_id: int) -> Optional[str]:
        for case_id, case in self.current_cases.items():
            if (
                user_id in {case.plaintiff_id, case.defendant_id}
                and case.status != CaseStatus.CLOSED
            ):
                logger.debug(
                    f"User `{user_id}` is associated with active case `{case_id}`."
                )
                return case_id
        logger.debug(f"No active case found for user `{user_id}`.")
        return None

    @lru_cache(maxsize=128)
    def find_case_number_by_channel_id(self, channel_id: int) -> Optional[str]:
        for case_id, case in self.current_cases.items():
            if channel_id in {case.thread_id, case.trial_thread_id}:
                logger.debug(
                    f"Channel `{channel_id}` is associated with case `{case_id}`."
                )
                return case_id
        logger.debug(f"No case associated with channel ID `{channel_id}`.")
        return None

    # Lock functions

    @asynccontextmanager
    async def case_lock_context(self, case_id: str) -> AsyncGenerator[None, None]:
        lock = self.get_case_lock(case_id)
        start_time = perf_counter()
        try:
            await asyncio.wait_for(lock.acquire(), timeout=5.0)
            self._lock_timestamps[case_id] = asyncio.get_event_loop().time()
            yield
        except TimeoutError:
            logger.error(f"Timeout acquiring lock for case {case_id}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error acquiring lock for case {case_id}: {e}")
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
        lock = asyncio.Lock()
        existing_lock = self.case_lock_cache.get(case_id)
        if existing_lock is not None:
            return existing_lock
        self.case_lock_cache[case_id] = lock
        return lock

    @asynccontextmanager
    async def case_context(self, case_id: str) -> AsyncGenerator[None, None]:
        async with self.case_lock_context(case_id):
            case = await self.repository.get_case(case_id)
            yield case
            if case:
                await self.repository.persist_case(case_id, case)
