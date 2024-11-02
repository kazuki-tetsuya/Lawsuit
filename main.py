from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import fcntl
import functools
import io
import itertools
import multiprocessing
import os
import random
import re
import secrets
import time
import tracemalloc
import types
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from enum import IntEnum, StrEnum, auto
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
from loguru import logger

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


logger.remove()
logger.add(
    sink=LOG_FILE,
    level="DEBUG",
    format=(
        "{time:YYYY-MM-DD HH:mm:ss.SSS ZZ} | "
        "{process}:{thread} | "
        "{level: <8} | "
        "{name}:{function}:{line} - "
        "{message}"
    ),
    filter=lambda record: (
        record["name"].startswith("extensions.github_d_com__kazuki388_s_Lawsuit.main")
    ),
    colorize=None,
    serialize=False,
    backtrace=True,
    diagnose=True,
    context=None,
    enqueue=False,
    catch=True,
    rotation="1 MB",
    retention=1,
    compression="gz",
    encoding="utf-8",
    mode="a",
)

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
            regex_flags=re.I,
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
        use_enum_values=True,
    )
    updated_at: datetime = pydantic.Field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0),
        frozen=True,
        use_enum_values=True,
    )

    def model_dump(self, exclude_unset: bool = False) -> Dict[str, Any]:
        data = pydantic.BaseModel.model_dump(self, exclude_unset=exclude_unset)
        for key, value in data.items():
            if isinstance(value, frozenset):
                data[key] = list(value)
            elif isinstance(value, dict):
                data[key] = {
                    k: list(v) if isinstance(v, frozenset) else v
                    for k, v in value.items()
                }
        return data


class Store:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.locks: collections.defaultdict[str, asyncio.Lock] = (
            collections.defaultdict(asyncio.Lock)
        )
        self.global_lock = asyncio.Lock()
        self.db_path = os.path.join(BASE_DIR, "cases.json")

    async def initialize_store(self) -> None:
        try:
            db_dir = os.path.dirname(self.db_path)
            await asyncio.to_thread(os.makedirs, db_dir, exist_ok=True)
            await self._ensure_db_file()
        except OSError as e:
            logger.critical(f"Failed to initialize store: {e}")
            raise RuntimeError("Store initialization failed") from e

    async def _ensure_db_file(self) -> None:
        if not os.path.exists(self.db_path):
            async with aiofiles.open(self.db_path, mode="w", encoding="utf-8") as f:
                await f.write("{}")
                await f.flush()
                os.fsync(f.fileno())
            logger.info(f"Created new JSON database file at {self.db_path}")

    @contextlib.asynccontextmanager
    async def _file_lock(self, mode: str, shared: bool = False) -> AsyncIterator[Any]:
        async with aiofiles.open(self.db_path, mode=mode) as f:
            lock_type = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
            await asyncio.to_thread(fcntl.flock, f.fileno(), lock_type | fcntl.LOCK_NB)
            try:
                yield f
            finally:
                await asyncio.to_thread(fcntl.flock, f.fileno(), fcntl.LOCK_UN)

    async def read_all(self) -> Dict[str, Dict[str, Any]]:
        async with self._file_lock("rb", shared=True) as f:
            return orjson.loads(await f.read() or b"{}")

    async def read_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        async with self._file_lock("rb", shared=True) as f:
            return (orjson.loads(await f.read() or b"{}")).get(case_id)

    async def write_case(self, case_id: str, case_data: Dict[str, Any]) -> None:
        async with self._file_lock("r+b") as f:
            data = orjson.loads(await f.read() or b"{}")
            data[case_id] = case_data
            serialized = orjson.dumps(
                data, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
            )
            await f.seek(0)
            await f.write(serialized)
            await f.truncate()
            await f.flush()
            os.fsync(f.fileno())

    async def delete_case(self, case_id: str) -> None:
        async with self._file_lock("r+b") as f:
            data = orjson.loads(await f.read() or b"{}")
            if case_id in data:
                del data[case_id]
                serialized = orjson.dumps(
                    data, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS
                )
                await f.seek(0)
                await f.write(serialized)
                await f.truncate()
                await f.flush()
                os.fsync(f.fileno())


class Repository(Generic[T]):
    def __init__(self, config: Config) -> None:
        self.config = config
        self.store = Store(config)
        self.cached_cases: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=1024, ttl=3600
        )
        self._case_locks: collections.defaultdict[str, asyncio.Lock] = (
            collections.defaultdict(asyncio.Lock)
        )
        self._initialization_lock: asyncio.Lock = asyncio.Lock()
        self._initialized: bool = False

    async def initialize_repository(self) -> None:
        if self._initialized:
            return
        async with self._initialization_lock:
            if not self._initialized:
                await self.store.initialize_store()
                self._initialized = True
                logger.debug("Repository initialized with optimized configuration")

    @contextlib.asynccontextmanager
    async def case_lock_manager(self, case_id: str) -> AsyncIterator[None]:
        if not isinstance(case_id, str):
            raise TypeError(f"case_id must be str, got {type(case_id)}")
        lock = self._case_locks[case_id]
        try:
            async with asyncio.timeout(5.0):
                await lock.acquire()
                yield
        except asyncio.TimeoutError:
            logger.critical(f"Potential deadlock detected for case_id: {case_id}")
            raise asyncio.TimeoutError(f"Lock acquisition timeout for case: {case_id}")
        finally:
            if lock.locked():
                lock.release()

    async def get_case_data(self, case_id: str) -> Optional[T]:
        if not self._initialized:
            await self.initialize_repository()
        try:
            if cached := self.cached_cases.get(case_id):
                return cached
            if case_data := await self.store.read_case(case_id):
                instance = self._create_case_instance(dict(sorted(case_data.items())))
                self.cached_cases[case_id] = instance
                return instance
            return None
        except Exception as e:
            logger.exception(f"Critical error in get_case_data for {case_id}: {e}")
            raise

    async def persist_case(self, case_id: str, case: T) -> None:
        if not isinstance(case, Data):
            raise TypeError(f"Expected Data instance, got {type(case)}")
        async with self.case_lock_manager(case_id):
            try:
                await self.store.write_case(
                    case_id, case.model_dump(exclude_unset=True)
                )
                self.cached_cases[case_id] = case
            except Exception as e:
                logger.critical(f"Failed to persist case {case_id}: {e}")
                raise

    async def update_case(self, case_id: str, **kwargs: Any) -> None:
        async with self.case_lock_manager(case_id):
            if not (case := await self.get_case_data(case_id)):
                raise KeyError(f"Case {case_id} not found")
            try:
                updated_case = await asyncio.to_thread(
                    self._update_case_instance, case, **kwargs
                )
                await self.persist_case(case_id, updated_case)
            except pydantic.ValidationError as e:
                logger.error(f"Validation failed for case {case_id} update: {e}")
                raise

    async def delete_case(self, case_id: str) -> None:
        async with self.case_lock_manager(case_id):
            try:
                await self.store.delete_case(case_id)
                self.cached_cases.pop(case_id, None)
                self._case_locks.pop(case_id, None)
            except Exception as e:
                logger.critical(f"Failed to delete case {case_id}: {e}")
                raise

    async def get_all_cases(self) -> Dict[str, T]:
        if not self._initialized:
            await self.initialize_repository()
        case_data = await self.store.read_all()
        return {
            case_id: self.cached_cases.setdefault(
                case_id, self._create_case_instance(dict(sorted(data.items())))
            )
            for case_id, data in case_data.items()
            if self._validate_case_data(case_id, data)
        }

    def _validate_case_data(self, case_id: str, data: Dict[str, Any]) -> bool:
        try:
            self._create_case_instance(dict(sorted(data.items())))
            return True
        except pydantic.ValidationError as e:
            logger.error(f"Validation error for case {case_id}: {e.errors()}")
            return False

    @staticmethod
    def _create_case_instance(data: Mapping[str, Any]) -> T:
        return Data(**dict(data))

    @staticmethod
    def _update_case_instance(case: T, **kwargs: Any) -> T:
        return Data(**{**case.__dict__, **kwargs})


# View


class View:

    PROVERBS: Tuple[str, ...] = (
        "Iuris prudentia est divinarum atque humanarum rerum notitia, iusti atque iniusti scientia (D. 1, 1, 10, 2).",
        "Iuri operam daturum prius nosse oportet, unde nomen iuris descendat. est autem a iustitia appellatum: nam, ut eleganter celsus definit, ius est ars boni et aequi (D. 1, 1, 1, pr.).",
        "Iustitia est constans et perpetua voluntas ius suum cuique tribuendi (D. 1, 1, 10, pr.).",
    )

    def __init__(self, bot: interactions.Client, config: Config) -> None:
        self.bot: interactions.Client = bot
        self.config: Config = config
        self.embed_cache: Dict[str, interactions.Embed] = collections.defaultdict(
            lambda: None
        )
        self.button_cache: Dict[str, List[interactions.Button]] = (
            collections.defaultdict(list)
        )
        self._proverb_selector: Callable[[date], str] = functools.lru_cache(
            maxsize=366
        )(self._create_proverb_selector())
        self.cached_cases: types.MappingProxyType[str, Data] = types.MappingProxyType(
            dict()
        )
        self._proverb_cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=max(366, len(self.PROVERBS)), ttl=86400, timer=time.monotonic
        )
        self._embed_lock: asyncio.Lock = asyncio.Lock()
        self._button_lock: asyncio.Lock = asyncio.Lock()
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
        cache_key = str(hash((title, description, color.value)))

        if cached := self.embed_cache.get(cache_key):
            return cached

        async with self._embed_lock:
            if cached := self.embed_cache.get(cache_key):
                return cached

            for attempt in range(retry_attempts):
                try:
                    guild = await self.bot.fetch_guild(self.config.GUILD_ID)
                    embed = interactions.Embed.__new__(interactions.Embed)
                    embed.__init__(
                        title=title,
                        description=description,
                        color=color.value,
                        timestamp=datetime.now(timezone.utc).replace(microsecond=0),
                    )

                    footer_text = guild.name if guild else "鍵政大舞台"
                    footer_icon = str(guild.icon.url) if guild and guild.icon else None
                    embed.set_footer(text=footer_text, icon_url=footer_icon)

                    self.embed_cache[cache_key] = embed
                    return embed

                except HTTPException as e:
                    if attempt == retry_attempts - 1:
                        logger.error(
                            f"Failed to create embed after {retry_attempts} attempts",
                            exc_info=True,
                        )
                        raise
                    await asyncio.sleep(1 << attempt)
                    continue

                except Exception as e:
                    logger.error(
                        f"Critical error in embed generation: {e}", exc_info=True
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
        backoff = functools.partial(min, 32)

        for attempt in itertools.count():
            if attempt >= retry_attempts:
                break

            try:
                with contextlib.suppress(asyncio.TimeoutError):
                    async with asyncio.timeout(5.0):
                        await ctx.send(embed=embed, ephemeral=True)
                        logger.debug(f"Response delivered on attempt {attempt + 1}")
                        return

            except HTTPException as exc:
                if exc.status != 404 or attempt == retry_attempts - 1:
                    logger.error(f"HTTP error in delivery: {exc}", exc_info=True)
                    raise
                try:
                    async with asyncio.timeout(5.0):
                        await ctx.send(embed=embed, ephemeral=True)
                        logger.debug("Followup sent for expired interaction")
                        return
                except (HTTPException, ConnectionError) as e:
                    if attempt == retry_attempts - 1:
                        logger.error(f"Failed followup: {e}", exc_info=True)
                        raise

            except (ConnectionError, ValueError) as e:
                if attempt == retry_attempts - 1:
                    logger.error(f"Error in delivery: {e}", exc_info=True)
                    raise

            await asyncio.sleep(backoff(2**attempt))

    async def send_error(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.deliver_response(ctx, "Error", message, EmbedColor.ERROR)

    async def send_success(
        self, ctx: interactions.InteractionContext, message: str
    ) -> None:
        await self.deliver_response(ctx, "Success", message, EmbedColor.INFO)

    @functools.cached_property
    def lawsuit_modal_store(self) -> cachetools.TTLCache:
        return cachetools.TTLCache(maxsize=2, ttl=3600)

    def create_lawsuit_modal(self, is_appeal: bool = False) -> interactions.Modal:
        cache_key = is_appeal
        if cache_key in self.lawsuit_modal_store:
            return self.lawsuit_modal_store[cache_key]

        fields = self.define_modal_fields(is_appeal)
        numbered_fields = [
            field.__class__(
                custom_id=f"{field.custom_id}_{i}",
                label=field.label,
                placeholder=field.placeholder,
                required=field.required,
                min_length=field.min_length,
                max_length=field.max_length,
            )
            for i, field in enumerate(fields)
        ]

        modal = interactions.Modal(
            *numbered_fields,
            title=("Appeal Form" if is_appeal else "Lawsuit Form"),
            custom_id=f"{'appeal' if is_appeal else 'lawsuit'}_form_modal",
        )

        self.lawsuit_modal_store[cache_key] = modal
        return modal

    @staticmethod
    def define_modal_fields(
        is_appeal: bool,
    ) -> tuple[Union[interactions.ShortText, interactions.ParagraphText], ...]:
        field_configs = (
            (
                ("case_number", "Case Number", "Enter the original case number")
                if is_appeal
                else ("defendant_id", "Defendant ID", "Enter the defendant's user ID")
            ),
            (
                ("appeal_reason", "Reason for Appeal", "Enter the reason for appeal")
                if is_appeal
                else ("accusation", "Accusation", "Enter the accusation")
            ),
        )

        fields = tuple(
            interactions.ShortText(
                custom_id=custom_id,
                label=label,
                placeholder=placeholder,
            )
            for custom_id, label, placeholder in field_configs
        ) + (
            interactions.ParagraphText(
                custom_id="facts",
                label="Facts",
                placeholder="Describe the relevant facts",
            ),
        )

        logger.debug(f"Defined optimized modal fields: is_appeal={is_appeal}")
        return fields

    @property
    def action_buttons_store(self) -> interactions.ActionRow:
        buttons = interactions.ActionRow(
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
            )
        )
        logger.debug("Created optimized action buttons")
        return buttons

    async def create_summary_embed(
        self, case_id: str, case: Data
    ) -> interactions.Embed:
        embed = await self.generate_embed(f"Case #{case_id}")
        embed.add_fields(
            *(
                interactions.EmbedField(
                    name=name,
                    value=(
                        " ".join(f"<@{j}>" for j in case.judges) or "Not yet assigned"
                        if name == "Presiding Judge"
                        else (
                            f"<@{getattr(case, f'{name.lower()}_id')}>"
                            if name in ("Plaintiff", "Defendant")
                            else getattr(case, name.lower()) or "None"
                        )
                    ),
                    inline=False,
                )
                for name in (
                    "Presiding Judge",
                    "Plaintiff",
                    "Defendant",
                    "Accusation",
                    "Facts",
                )
            )
        )
        logger.debug(f"Created optimized summary embed: case_id={case_id}")
        return embed

    def create_action_buttons(self, case_id: str) -> List[interactions.ActionRow]:
        buttons = [
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
        logger.debug(f"Created optimized action buttons: case_id={case_id}")
        return buttons

    async def create_trial_privacy_buttons(
        self, case_id: str
    ) -> Tuple[interactions.Button, ...]:
        cache_key = f"trial_privacy_{case_id}"

        try:
            return tuple(self.button_cache[cache_key])
        except KeyError:
            async with self._button_lock:
                if cache_key in self.button_cache:
                    return tuple(self.button_cache[cache_key])

                buttons = tuple(
                    map(
                        lambda x: interactions.Button(
                            style=getattr(interactions.ButtonStyle, x[0]),
                            label=x[1],
                            custom_id=f"public_trial_{x[2]}_{case_id}",
                        ),
                        (("SUCCESS", "Yes", "yes"), ("DANGER", "No", "no")),
                    )
                )

                self.button_cache[cache_key] = list(buttons)
                logger.debug(f"Created and cached privacy buttons: case_id={case_id}")
                return buttons

    def create_end_trial_button(self, case_id: str) -> interactions.Button:
        button = interactions.Button(
            style=interactions.ButtonStyle.DANGER,
            label="End Trial",
            custom_id=f"end_trial_{case_id}",
        )
        logger.debug(f"Created optimized end trial button: case_id={case_id}")
        return button

    def create_user_management_buttons(
        self, user_id: int, _cache: Dict[int, Tuple[interactions.Button, ...]] = {}
    ) -> Tuple[interactions.Button, ...]:
        return tuple(
            map(
                lambda x: interactions.Button(
                    style=x[2], label=x[0], custom_id=f"{x[1]}_{user_id}"
                ).__tap__(
                    lambda b: logger.debug(
                        f"Created button {b.label} for user {user_id}"
                    )
                ),
                (
                    ("Mute", "mute", interactions.ButtonStyle.PRIMARY),
                    ("Unmute", "unmute", interactions.ButtonStyle.SUCCESS),
                ),
            )
        )

    def create_message_management_buttons(
        self, message_id: int
    ) -> Tuple[interactions.Button, ...]:
        return tuple(
            interactions.Button(
                style=getattr(interactions.ButtonStyle, style),
                label=label,
                custom_id=f"{action}_{message_id}",
            ).__tap__(
                lambda b: logger.debug(
                    f"Created {b.label} button for message {message_id}"
                )
            )
            for style, label, action in (
                ("PRIMARY", "Pin", "pin"),
                ("DANGER", "Delete", "delete"),
            )
        )

    def create_evidence_action_buttons(
        self, case_id: str, user_id: int
    ) -> Tuple[interactions.Button, ...]:
        return tuple(
            interactions.Button(
                style=getattr(interactions.ButtonStyle, style),
                label=label,
                custom_id=f"{action}_evidence_{case_id}_{user_id}",
            ).__tap__(
                lambda b: logger.debug(
                    f"Created optimized {b.label} evidence button: case_id={case_id}, user_id={user_id}"
                )
            )
            for style, label, action in (
                ("SUCCESS", "Make Public", "approve"),
                ("DANGER", "Keep Private", "reject"),
            )
        )

    def _create_proverb_selector(self) -> Callable[[date], str]:
        proverb_count: int = len(self.PROVERBS)
        shuffled_indices: tuple[int, ...] = tuple(
            random.SystemRandom().sample(range(proverb_count), proverb_count)
        )
        logger.debug("Created optimized proverb selector with secure randomization")

        def select_proverb(current_date: date) -> str:
            index_position = current_date.toordinal() % proverb_count
            proverb = self.PROVERBS[shuffled_indices[index_position]]
            logger.debug(f"Selected optimized proverb for {current_date}")
            return proverb

        return select_proverb

    def select_daily_proverb(self, current_date: Optional[date] = None) -> str:
        cache = self._proverb_cache
        selector = self._proverb_selector

        today = current_date or date.today()
        cache_key = today.toordinal()

        if cache_key in cache:
            return cache[cache_key]

        proverb = selector(today)
        cache[cache_key] = proverb
        logger.debug(
            f"{'Cache hit' if cache_key in cache else 'Selected and cached'} proverb for {today}"
        )
        return proverb

    def clear_caches(self) -> None:
        try:
            for lock in (self._embed_lock, self._button_lock):
                if lock.locked():
                    lock.release()

            self.embed_cache.clear()
            self.button_cache.clear()

            tuple(
                map(
                    dict.clear,
                    (
                        self.lawsuit_modal_store,
                        self.action_buttons_store,
                        self._proverb_cache,
                    ),
                )
            )

            logger.info("Cache clearing completed with optimal thread safety")

        except Exception as e:
            logger.critical(f"Critical error during cache clearing: {e}")
            raise RuntimeError("Cache clearing failed") from e


async def user_is_judge(ctx: interactions.BaseContext) -> bool:
    judge_id = Config().JUDGE_ROLE_ID
    result = next((True for r in ctx.author.roles if r.id == judge_id), False)
    logger.debug(f"Ultra-optimized judge role validation: {result}")
    return result


# Controller


class Lawsuit(interactions.Extension):
    def __init__(self, bot: interactions.Client) -> None:
        self._cleanup_task = None
        self.bot = bot
        self.config = Config()
        self.store = Store(self.config)
        self.repo: Repository = Repository(self.config)
        self.view = View(bot, self.config)
        self.cached_cases: dict[str, Repository] = {}
        self.case_data_cache = cachetools.TTLCache(maxsize=1000, ttl=300)
        self._case_locks = sortedcontainers.SortedDict()
        self._lock_timestamps = sortedcontainers.SortedDict()
        self._case_lock = asyncio.Lock()
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
        self.case_locks = cachetools.TTLCache(maxsize=1000, ttl=300)
        self._init_task = asyncio.create_task(
            self.initialize_background_tasks(), name="LawsuitInitialization"
        )
        self._init_task.add_done_callback(self.handle_init_exception)

    # Tasks

    async def initialize_background_tasks(self) -> None:
        tasks = {
            "_fetch_cases": self._fetch_cases,
            "_daily_embed_update": self._daily_embed_update,
            "_process_task_queue": self._process_task_queue,
        }
        for name, coro in tasks.items():
            task = asyncio.create_task(coro(), name=name)
            task.add_done_callback(self._handle_task_exception)

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

    def _handle_task_exception(self, task: asyncio.Task[Any]) -> None:
        match task:
            case _ if task.cancelled():
                logger.info(f"Task {task.get_name()} cancelled gracefully")
                return
            case _ if (exc := task.exception()):
                match exc:
                    case asyncio.CancelledError():
                        logger.info(
                            f"Task {task.get_name()} cancelled via CancelledError"
                        )
                    case _:
                        logger.critical(
                            f"Critical task failure in {task.get_name()}",
                            exc_info=exc,
                            extra={"task_name": task.get_name()},
                        )
                        self.restart_task(task)

    def restart_task(self, task: asyncio.Task[Any]) -> None:
        task_name = task.get_name().lower()
        if not (task_method := getattr(self, f"_{task_name}", None)):
            logger.error(
                f"Task restart failed - method not found: {task_name}",
                extra={"task_name": task_name},
            )
            return
        new_task = asyncio.create_task(task_method(), name=task.get_name())
        new_task.add_done_callback(self._handle_task_exception)
        logger.info(f"Task {task.get_name()} restarted successfully")

    async def cleanup_locks(self) -> None:
        async with self._case_lock:
            self._case_locks.clear()
            self._lock_timestamps.clear()
        logger.info("Lock cleanup completed successfully")

    def __del__(self) -> None:
        if cleanup_task := getattr(self, "_cleanup_task", None):
            if isinstance(cleanup_task, asyncio.Task) and not cleanup_task.done():
                cleanup_task.cancel()
                logger.info("Cleanup task cancelled during deletion")

    async def _fetch_cases(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                cases = await self.repo.get_all_cases()
                self.cached_cases.clear()
                self.cached_cases.update(cases)
                logger.info(
                    "Cases fetched successfully",
                    extra={"case_count": len(self.cached_cases)},
                )
                await asyncio.sleep(300)
            except Exception as e:
                logger.exception(
                    "Case fetch failed", exc_info=True, extra={"error": str(e)}
                )
                self.cached_cases.clear()
                logger.info("Cache cleared due to fetch failure")
                await asyncio.sleep(60)

    async def _process_task_queue(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                if task_callable := await asyncio.wait_for(
                    self.case_task_queue.get(), timeout=1.0
                ):
                    await asyncio.shield(task_callable())
                    self.case_task_queue.task_done()
                    logger.debug("Task processed successfully")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.exception(
                    "Task processing failed", exc_info=True, extra={"error": str(e)}
                )
                self.case_task_queue.task_done()

    async def _daily_embed_update(self) -> None:
        while not self.shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                await self._update_lawsuit_button_embed()
            except asyncio.CancelledError:
                logger.info("Embed update cancelled")
                break
            except Exception as e:
                logger.error(
                    "Embed update failed", exc_info=True, extra={"error": str(e)}
                )

    async def _update_lawsuit_button_embed(self) -> None:
        logger.info("Starting embed update")
        if not self.lawsuit_button_message_id:
            logger.warning("Button message ID not set")
            return

        try:
            channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
            message = await channel.fetch_message(self.lawsuit_button_message_id)
            daily_proverb = self.view.select_daily_proverb()

            embed = await self.view.generate_embed(
                title=f"{daily_proverb}",
                description="Click the button to file a lawsuit or appeal.",
            )

            await message.edit(embeds=[embed])
            logger.info("Embed updated successfully")

        except NotFound:
            logger.error("Button message not found")
        except HTTPException as e:
            logger.exception(
                "HTTP error during embed update", exc_info=True, extra={"error": str(e)}
            )
        except Exception as e:
            logger.exception(
                "Unexpected embed update failure",
                exc_info=True,
                extra={"error": str(e)},
            )

    # Listeners

    @interactions.listen(NewThreadCreate)
    async def on_thread_create(self, event: NewThreadCreate) -> None:
        if event.thread.parent_id != self.config.COURTROOM_CHANNEL_ID:
            return

        try:
            await self.delete_thread_created_message(
                event.thread.parent_id, event.thread
            )
            logger.debug(
                f"Thread {event.thread.id} deleted successfully from courtroom channel"
            )
        except Exception as e:
            logger.exception(
                "Thread creation handling failed",
                exc_info=True,
                extra={"thread_id": event.thread.id, "error": str(e)},
            )
            raise

    @interactions.listen(MessageCreate)
    async def on_message_create(self, event: MessageCreate) -> None:
        if event.message.author.id == self.bot.user.id:
            return

        try:
            handler = (
                self.handle_direct_message_evidence
                if isinstance(event.message.channel, interactions.DMChannel)
                else self.handle_channel_message
            )
            await handler(event.message)
            logger.debug(
                f"{handler.__name__.replace('handle_', '').title()} processed",
                extra={
                    (
                        "user_id"
                        if isinstance(event.message.channel, interactions.DMChannel)
                        else "channel_id"
                    ): (
                        event.message.author.id
                        if isinstance(event.message.channel, interactions.DMChannel)
                        else event.message.channel.id
                    )
                },
            )
        except Exception as e:
            logger.exception(
                "Message processing failed",
                exc_info=True,
                extra={"message_id": event.message.id, "error": str(e)},
            )
            raise

    @interactions.listen(ExtensionUnload)
    async def on_extension_unload(self) -> None:
        logger.info("Initiating Lawsuit extension unload sequence")
        self.shutdown_event.set()

        if self._cleanup_task and not self._cleanup_task.done():
            try:
                self._cleanup_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(self._cleanup_task, timeout=5.0)
                logger.debug("Cleanup task cancelled successfully")
            except asyncio.TimeoutError:
                logger.error("Cleanup task cancellation timed out")
            except Exception as e:
                logger.exception(
                    "Cleanup task cancellation failed",
                    exc_info=True,
                    extra={"error": str(e)},
                )
                raise

        await self.cleanup_locks()
        logger.info("Lawsuit extension unloaded successfully")

    # Component

    @interactions.component_callback("initiate_lawsuit_button")
    async def initiate_lawsuit(self, ctx: interactions.ComponentContext) -> None:
        try:
            if await self.has_ongoing_lawsuit(ctx.author.id):
                await self.view.send_error(ctx, "You already have an ongoing lawsuit")
                logger.debug(f"Duplicate lawsuit attempt by user {ctx.author.id}")
                return
            await ctx.send_modal(self.view.create_lawsuit_modal())
            logger.info("Lawsuit modal sent", extra={"user_id": ctx.author.id})
        except Exception as e:
            logger.exception(
                "Lawsuit initiation failed",
                exc_info=True,
                extra={"user_id": ctx.author.id, "error": str(e)},
            )
            await self.view.send_error(
                ctx, "An unexpected error occurred. Please try again later."
            )
            raise

    @interactions.modal_callback("lawsuit_form_modal")
    async def submit_lawsuit_form(self, ctx: interactions.ModalContext) -> None:
        try:
            await ctx.defer(ephemeral=True)
            logger.debug(f"Processing lawsuit form: {ctx.responses}")

            if not (
                responses := {k: v.strip() for k, v in ctx.responses.items()}
            ) or not all(responses.values()):
                await self.view.send_error(
                    ctx, "Please ensure all fields are properly filled"
                )
                return

            defendant_id_str = responses.get("defendant_id_0", "")
            try:
                defendant_id = int("".join(filter(str.isdigit, defendant_id_str)))
                if defendant_id == ctx.author.id:
                    await self.view.send_error(ctx, "Self-litigation is not permitted")
                    return
                if not (user := await self.bot.fetch_user(defendant_id)):
                    await self.view.send_error(ctx, "Invalid defendant ID provided")
                    return
            except (ValueError, NotFound):
                await self.view.send_error(
                    ctx, "Invalid defendant ID or user not found"
                )
                return

            sanitized_data = {
                "defendant_id": str(defendant_id),
                "accusation": responses.get("accusation_1", ""),
                "facts": responses.get("facts_2", ""),
            }

            try:
                case_id, case = await self.create_new_case(
                    ctx, sanitized_data, defendant_id
                )
                if not case_id or not case:
                    await self.view.send_error(ctx, "Failed to create case")
                    return

                await self.setup_mediation_thread(case_id, case)
                await self.notify_judges(case_id, ctx.guild_id)
                await self.view.send_success(
                    ctx, f"Mediation room #{case_id} created successfully"
                )

            except Exception as e:
                logger.exception("Failed to setup case", exc_info=True)
                await self.view.send_error(ctx, "Failed to setup the case")
                return

        except Exception as e:
            logger.exception(
                "Lawsuit submission failed", exc_info=True, extra={"error": str(e)}
            )
            await self.view.send_error(
                ctx, "An unexpected error occurred. Please try again later."
            )

    @interactions.component_callback("dismiss_*", "accept_*", "withdraw_*")
    async def handle_case_actions(self, ctx: interactions.ComponentContext) -> None:
        action, case_id = ctx.custom_id.split("_", 1)
        await self.process_case_action(ctx, CaseAction[action.upper()], case_id)

    @interactions.component_callback("public_trial_*")
    async def handle_trial_privacy(self, ctx: interactions.ComponentContext) -> None:
        try:
            remainder = ctx.custom_id.removeprefix("public_trial_")
            is_public_str, case_id = remainder.split("_", 1)

            async with self.repo.case_lock_manager(case_id) as case:
                if not case:
                    logger.warning(f"Case {case_id} not found")
                    await self.view.send_error(ctx, "Case not found")
                    return
                await self.create_trial_thread(
                    ctx=ctx,
                    case_id=case_id,
                    case=case,
                    is_public=is_public_str.lower() == "yes",
                )
                logger.info(
                    "Trial privacy set",
                    extra={
                        "case_id": case_id,
                        "is_public": is_public_str.lower() == "yes",
                    },
                )
        except ValueError:
            logger.error(
                "Invalid format", exc_info=True, extra={"custom_id": ctx.custom_id}
            )
            await self.view.send_error(ctx, "Invalid request format")
        except Exception as e:
            logger.exception(
                "Trial privacy handling failed", exc_info=True, extra={"error": str(e)}
            )
            await self.view.send_error(
                ctx, "Failed to set trial privacy. Please try again later."
            )
            raise

    @interactions.component_callback("mute_*", "unmute_*")
    async def handle_user_mute(self, ctx: interactions.ComponentContext) -> None:
        action_str, user_id_str = ctx.custom_id.split("_", 1)
        if not (case_id := await self._validate_user_permissions(ctx)):
            logger.warning("Case not found for channel")
            await self.view.send_error(ctx, "Associated case not found")
            return
        try:
            await self.process_mute_action(
                ctx=ctx,
                case_id=case_id,
                target_id=int(user_id_str),
                mute=action_str == "mute",
            )
        except ValueError:
            logger.error(f"Invalid user ID: {user_id_str}")
            await self.view.send_error(ctx, "Invalid user ID format")
            raise

    @interactions.component_callback("pin_*", "delete_*")
    async def handle_message_action(self, ctx: interactions.ComponentContext) -> None:
        action_str, message_id_str = ctx.custom_id.split("_", 1)
        if not (case_id := await self._validate_user_permissions(ctx)):
            logger.warning("Case not found for channel")
            await self.view.send_error(ctx, "Associated case not found")
            return
        try:
            await self.process_message_action(
                ctx=ctx,
                case_id=case_id,
                target_id=int(message_id_str),
                action_type=MessageAction(action_str),
            )
        except ValueError:
            logger.error(f"Invalid message ID: {message_id_str}")
            await self.view.send_error(ctx, "Invalid message ID format")
            raise

    @interactions.component_callback("approve_evidence_*", "reject_evidence_*")
    async def handle_evidence_action(self, ctx: interactions.ComponentContext) -> None:
        try:
            action_str, remainder = ctx.custom_id.split("_evidence_", 1)
            case_id, user_id_str = remainder.split("_", 1)
            await self.process_judge_evidence_decision(
                ctx=ctx,
                action=(
                    EvidenceAction.APPROVED
                    if action_str == "approve"
                    else EvidenceAction.REJECTED
                ),
                case_id=case_id,
                user_id=int(user_id_str),
            )
        except ValueError:
            logger.error(f"Invalid format: {ctx.custom_id}")
            await self.view.send_error(ctx, "Invalid request format")
            raise

    @interactions.component_callback("end_trial_*")
    async def handle_end_trial(self, ctx: interactions.ComponentContext) -> None:
        await self.process_case_action(
            ctx, CaseAction.CLOSE, ctx.custom_id.removeprefix("end_trial_")
        )

    # Command handlers

    module_base: interactions.SlashCommand = interactions.SlashCommand(
        name="lawsuit",
        description="Lawsuit management system",
    )

    @module_base.subcommand(
        "memory",
        sub_cmd_description="Print top memory allocations",
    )
    @interactions.check(user_is_judge)
    async def show_memory_allocations(self, ctx: interactions.SlashContext) -> None:
        try:
            async with asyncio.timeout(5.0):
                tracemalloc.start(50)
                try:
                    snapshot = tracemalloc.take_snapshot()
                    top_stats = sorted(
                        snapshot.statistics("traceback"),
                        key=lambda x: x.size,
                        reverse=True,
                    )[:10]
                    chunks: list[str] = []
                    current_chunk: list[str] = []
                    current_length = 0

                    for stat in top_stats:
                        stat_content = (
                            f"{stat.count:>8,d} allocs | {stat.size/1_048_576:>6.2f} MiB | "
                            f"{stat.size/(stat.count or 1)/1024:>6.2f} KiB/alloc\n"
                            f"{stat.traceback.format(limit=5)}\n"
                        )

                        if current_length + len(stat_content) + 6 > 4000:
                            chunks.append("```py\n" + "".join(current_chunk) + "```")
                            current_chunk = []
                            current_length = 0

                        current_chunk.append(stat_content)
                        current_length += len(stat_content)

                    if current_chunk:
                        chunks.append("```py\n" + "".join(current_chunk) + "```")

                    for chunk in chunks:
                        await ctx.send(chunk)

                    logger.debug("Memory analysis completed successfully")
                finally:
                    tracemalloc.stop()
        except* (Exception, asyncio.TimeoutError) as eg:
            logger.exception("Memory analysis failed", exc_info=True)
            await self.view.send_error(
                ctx,
                f"Analysis failed: {type(eg.__cause__).__name__} - {str(eg.__cause__)[:100]}",
            )

    @module_base.subcommand(
        "dispatch",
        sub_cmd_description="Deploy the lawsuit management interface",
    )
    @interactions.check(user_is_judge)
    async def deploy_lawsuit_button(self, ctx: interactions.SlashContext) -> None:
        await ctx.defer(ephemeral=True)
        try:
            async with asyncio.timeout(5.0):
                action_row, daily_proverb = (
                    self.view.action_buttons_store,
                    self.view.select_daily_proverb(),
                )

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
                    extra={
                        k: v
                        for k, v in {
                            "user_id": ctx.author.id,
                            "channel_id": ctx.channel_id,
                            "message_id": message.id,
                        }.items()
                    },
                )
        except* Exception as eg:
            match type(eg.__cause__):
                case HTTPException() as exc if exc.status == 404:
                    logger.warning("Interaction expired")
                case HTTPException() as exc:
                    logger.error(
                        "HTTP error in lawsuit deployment: %s",
                        exc.status,
                        exc_info=True,
                    )
                case _:
                    logger.exception(
                        "Lawsuit interface deployment failed",
                        exc_info=True,
                    )
            await self.view.send_error(ctx, "Failed to deploy interface")

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
                if not (case_id := self.find_case_number_by_channel_id(ctx.channel_id)):
                    raise ValueError("No associated case found")

                interaction_action = RoleAction(action.lower())
                await self.manage_case_role(
                    ctx,
                    case_id,
                    interaction_action,
                    role,
                    user,
                )

                logger.info(
                    "Role management completed",
                    extra={
                        k: v
                        for k, v in {
                            "action": (
                                "assigned to"
                                if interaction_action == RoleAction.ASSIGN
                                else "revoked from"
                            ),
                            "role": role,
                            "user_id": user.id,
                            "case_id": case_id,
                            "moderator_id": ctx.author.id,
                        }.items()
                    },
                )
        except* Exception as eg:
            error_msg = next(
                (
                    msg
                    for t, msg in {
                        ValueError: "Invalid configuration",
                        asyncio.TimeoutError: "Operation timed out",
                    }.items()
                    if isinstance(eg.__cause__, t)
                ),
                f"Unexpected error: {type(eg.__cause__).__name__}",
            )

            logger.exception(error_msg, exc_info=True)
            await self.view.send_error(ctx, error_msg)

    @interactions.message_context_menu(name="Message in Lawsuit")
    async def message_management_menu(
        self, ctx: interactions.ContextMenuContext
    ) -> None:
        try:
            if not await user_is_judge(ctx):
                await self.view.send_error(ctx, "Only judges can manage messages.")
                return

            if not (case_id := self.find_case_number_by_channel_id(ctx.channel_id)):
                await self.view.send_error(
                    ctx, "This command can only be used in case threads."
                )
                return

            await ctx.send(
                "Select an action:",
                components=[
                    interactions.ActionRow(
                        *self.create_message_management_buttons(ctx.target.id)
                    )
                ],
                ephemeral=True,
            )

            logger.info(
                "Message management menu opened",
                extra={
                    k: v
                    for k, v in {
                        "case_id": case_id,
                        "message_id": ctx.target.id,
                        "user_id": ctx.author.id,
                    }.items()
                },
            )

        except Exception as e:
            logger.exception(
                "Failed to open message management menu",
                extra={"message_id": ctx.target.id},
                exc_info=e,
            )
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

            if not (case_id := self.find_case_number_by_channel_id(ctx.channel_id)):
                await self.view.send_error(
                    ctx, "This command can only be used in case threads."
                )
                return

            await ctx.send(
                "Select an action:",
                components=[
                    interactions.ActionRow(
                        *self.create_user_management_buttons(ctx.target.id)
                    )
                ],
                ephemeral=True,
            )

            logger.info(
                "User management menu opened",
                extra={
                    k: v
                    for k, v in {
                        "case_id": case_id,
                        "target_user_id": ctx.target.id,
                        "user_id": ctx.author.id,
                    }.items()
                },
            )

        except Exception as e:
            logger.exception(
                "Failed to open user management menu",
                extra={"target_user_id": ctx.target.id},
                exc_info=e,
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
                case = await tg.create_task(self.repo.get_case_data(case_id))
                guild = await tg.create_task(self.bot.fetch_guild(self.config.GUILD_ID))

                if not case:
                    logger.warning("Case ID '%s' not found.", case_id)
                    await self.view.send_error(ctx, "Case not found.")
                    return

                member = await guild.fetch_member(ctx.author.id)
                logger.debug(
                    "Fetched member '%s' from guild '%s'.", member.id, guild.id
                )
                if handler := self.case_handlers.get(action):
                    await handler(ctx, case_id, case, member)
                    logger.info(
                        "Successfully executed handler for action '%s' on case '%s'",
                        action,
                        case_id,
                    )
                else:
                    logger.error("No handler found for action '%s'.", action)
                    await self.view.send_error(ctx, "Invalid action specified.")

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
            async with asyncio.TaskGroup() as tg:
                judge_check = tg.create_task(self.user_is_judge(user))
                if not await judge_check:
                    logger.warning(
                        "User '%s' lacks judge role for case '%s'.", user.id, case_id
                    )
                    await self.view.send_error(ctx, "Insufficient permissions.")
                    return

                await tg.create_task(self.add_judge_to_case(ctx, case_id))
                logger.info(
                    "User '%s' assigned as judge to case '%s'.", user.id, case_id
                )

        except* (asyncio.TimeoutError, asyncio.CancelledError) as eg:
            logger.error(
                "Timeout in file case request for case ID '%s': %s", case_id, eg
            )
            await self.view.send_error(ctx, "Operation timed out. Please try again.")
        except* Exception as eg:
            logger.exception(
                "Error handling file case request for case ID '%s': %s", case_id, eg
            )
            await self.view.send_error(
                ctx,
                f"An error occurred while processing your request: {type(eg).__name__}",
            )

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
            permission_check = await self.user_has_permission_for_closure(
                ctx.author.id, case_id, member
            )
            if not permission_check:
                logger.warning(
                    "User '%s' lacks permissions to close case '%s'.",
                    ctx.author.id,
                    case_id,
                )
                await self.view.send_error(ctx, "Insufficient permissions.")
                return

            action_name: str = action.value.capitalize()
            new_status: str = action.value
            case_status = CaseStatus(new_status)

            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(self.update_case_status(case_id, case, case_status)),
                    tg.create_task(self.notify_case_closure(ctx, case_id, action_name)),
                    tg.create_task(self.enqueue_case_file_save(case_id, case)),
                ]

            logger.info(
                "Successfully closed case ID '%s' with action '%s' by user '%s'.",
                case_id,
                action_name,
                ctx.author.id,
            )

        except* Exception as eg:
            error_msg = f"Failed to close the case: {type(eg).__name__}"
            logger.exception(
                "Error during case closure for case ID '%s': %s", case_id, eg
            )
            await self.view.send_error(ctx, error_msg)

    async def update_case_status(
        self, case_id: str, case: Data, new_status: CaseStatus
    ) -> None:
        logger.debug("Updating status of case ID '%s' to '%s'.", case_id, new_status)
        case.status = new_status

        thread_ids = tuple(filter(None, (case.thread_id, case.trial_thread_id)))
        if not thread_ids:
            return

        try:
            tasks = {
                thread_id: asyncio.create_task(self.archive_thread(thread_id))
                for thread_id in thread_ids
            }

            done, _ = await asyncio.wait(tasks.values())

            for thread_id, task in tasks.items():
                try:
                    await task
                    logger.info("Successfully archived thread %s", thread_id)
                except Exception as e:
                    logger.error("Failed to archive thread %s: %s", thread_id, e)

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
            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(
                        self.notify_case_participants(case_id, notification)
                    ),
                    tg.create_task(
                        self.view.send_success(
                            ctx, f"Case number {case_id} successfully {action_name}."
                        )
                    ),
                ]
                await next(iter(tasks))
                await tasks[1]

            logger.info(
                "Successfully notified participants and user about closure of case ID '%s'.",
                case_id,
            )
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
            if not await self.is_user_assigned_to_case(
                ctx.author.id, case_id, role="judges"
            ):
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        logger.warning(
                            "User '%s' is not assigned to case ID '%s'.",
                            ctx.author.id,
                            case_id,
                        )
                    )
                    tg.create_task(
                        self.view.send_error(ctx, "Insufficient permissions.")
                    )
                return

            async with asyncio.TaskGroup() as tg:
                embed_task = tg.create_task(
                    self.view.generate_embed(title="Public or Private Trial")
                )
                buttons = self.view.create_trial_privacy_buttons(case_id)

                await ctx.send(
                    embeds=[await embed_task],
                    components=[interactions.ActionRow(*buttons)],
                )

                tg.create_task(
                    logger.info(
                        "Sent trial privacy options for case ID '%s' to user '%s'.",
                        case_id,
                        ctx.author.id,
                    )
                )

        except* Exception as eg:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    logger.exception(
                        "Error handling accept case request for case ID '%s': %s",
                        case_id,
                        eg,
                    )
                )
                tg.create_task(
                    self.view.send_error(
                        ctx, f"Failed to process request: {type(eg).__name__}"
                    )
                )

    async def end_trial(self, ctx: interactions.ComponentContext) -> None:
        logger.debug("Handling end trial action with custom ID '%s'.", ctx.custom_id)

        if match := re.fullmatch(r"^end_trial_([a-f0-9]{12})$", ctx.custom_id):
            case_id = match.group(1)
            logger.info(
                "End trial initiated for case ID '%s' by user '%s'.",
                case_id,
                ctx.author.id,
            )
            await self.process_case_action(ctx, CaseAction.CLOSE, case_id)
        else:
            logger.warning(
                "Invalid custom ID '%s' received for end trial action.", ctx.custom_id
            )
            await self.view.send_error(ctx, "Invalid interaction data.")

    # Thread functions

    @staticmethod
    async def add_members_to_thread(
        thread: interactions.GuildText, member_ids: FrozenSet[int]
    ) -> None:
        async def _add_member(member_id: int) -> None:
            try:
                async with asyncio.timeout(5.0):
                    user = await thread.guild.fetch_member(member_id)
                    await thread.add_member(user)
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
            [tg.create_task(_add_member(mid)) for mid in member_ids]
            await asyncio.wait(
                [t for t in tg._tasks], return_when=asyncio.ALL_COMPLETED
            )

    async def add_participants_to_thread(
        self, thread: interactions.GuildText, case: Data
    ) -> None:
        try:
            async with asyncio.timeout(10.0):
                guild = await self.bot.fetch_guild(self.config.GUILD_ID)
                judges = await self.get_judges(guild)

                participants = frozenset(
                    itertools.chain((case.plaintiff_id, case.defendant_id), judges)
                )

                logger.debug(
                    "Adding participants to thread",
                    extra={
                        "thread_id": thread.id,
                        "participant_count": len(participants),
                    },
                )

                await self.add_members_to_thread(thread, participants)

                logger.info(
                    "Participants added successfully",
                    extra={
                        "thread_id": thread.id,
                        "case_id": case.id,
                    },
                )

        except* asyncio.TimeoutError as eg:
            logger.error(
                "Operation timed out",
                extra={"thread_id": thread.id, "error": str(eg)},
            )
            raise RuntimeError("Thread participant addition timed out") from eg
        except* Exception as eg:
            logger.exception(
                "Critical error adding participants",
                extra={"thread_id": thread.id, "error": str(eg)},
            )
            raise

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

                new_judges = frozenset(
                    itertools.chain(locked_case.judges, (ctx.author.id,))
                )

                async with asyncio.TaskGroup() as tg:
                    notification = f"{ctx.author.display_name} assigned as judge to Case #{case_id}"

                    tasks = (
                        tg.create_task(
                            self.repo.update_case(case_id, judges=new_judges)
                        ),
                        tg.create_task(
                            self.notify_case_participants(case_id, notification)
                        ),
                        tg.create_task(
                            self._update_thread_permissions(case_id, ctx.author.id)
                        ),
                        tg.create_task(
                            self.view.send_success(
                                ctx, f"Added as judge to Case #{case_id}"
                            )
                        ),
                    )

                    await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

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

    async def _update_thread_permissions(self, case_id: str, judge_id: int) -> None:
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
                    for tid in (
                        getattr(case, "thread_id", None),
                        getattr(case, "trial_thread_id", None),
                    )
                    if tid is not None
                }

                if not thread_ids:
                    logger.warning("No threads found", extra={"case_id": case_id})
                    return

                async with asyncio.TaskGroup() as tg:
                    for thread_id in thread_ids:
                        tg.create_task(
                            (await self.bot.fetch_channel(thread_id)).edit_permission(
                                permission_overwrite,
                                reason=f"Adding judge {judge_id} to case {case_id}",
                            )
                        )
                        logger.debug(
                            "Thread permissions updated",
                            extra={"thread_id": thread_id, "judge_id": judge_id},
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
        action_str = ("un" * (not mute)) + "muted"

        async with self.repo.case_lock_manager(case_id):
            case = await self.repo.get_case_data(case_id)
            if case is None:
                raise ValueError(f"Case {case_id} not found")

            is_muted = target_id in case.mute_list
            if is_muted == mute:
                await self.view.send_error(
                    ctx, f"User <@{target_id}> is already {action_str}."
                )
                return

            updated_mute_list = frozenset(
                {*case.mute_list, target_id}
                if mute
                else {x for x in case.mute_list if x != target_id}
            )

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self.repo.update_case(
                        case_id, mute_list=tuple(sorted(updated_mute_list))
                    )
                )
                tg.create_task(
                    self.view.send_success(
                        ctx, f"User <@{target_id}> has been {action_str}."
                    )
                )
                tg.create_task(
                    self.notify_case_participants(
                        case_id,
                        f"User <@{target_id}> has been {action_str} by <@{ctx.author.id}>.",
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
                channel_task = tg.create_task(self.bot.fetch_channel(ctx.channel_id))
                channel = await channel_task
                if not channel:
                    raise ValueError(f"Channel {ctx.channel_id} not found")
                message_task = tg.create_task(channel.fetch_message(target_id))
                message = await message_task
                if not message:
                    raise ValueError(f"Message {target_id} not found")

            action_map = {
                MessageAction.PIN.value: (message.pin, "ned"),
                MessageAction.DELETE.value: (message.delete, "d"),
            }

            try:
                action_func, suffix = action_map[action_type]
            except KeyError:
                raise ValueError(f"Invalid action_type: {action_type}")

            action_str = f"{action_type}{suffix}"
            await action_func()

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self.view.send_success(ctx, f"Message has been {action_str}.")
                )
                tg.create_task(
                    self.notify_case_participants(
                        case_id,
                        f"A message has been {action_str} by <@{ctx.author.id}> in case {case_id}.",
                    )
                )

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
        await self._send_dm_with_fallback(user_id, embed=embed)

    async def notify_case_participants(self, case_id: str, message: str) -> None:
        case = await self.repo.get_case_data(case_id)
        if not case:
            logger.warning(
                "Case %s not found when attempting to notify participants", case_id
            )
            return

        participants: frozenset[int] = frozenset(
            {case.plaintiff_id, case.defendant_id} | case.judges
        )
        embed = await self.view.generate_embed(
            title=f"Case {case_id} Update", description=message
        )

        await self._notify_users(
            user_ids=participants,
            notification_func=functools.partial(
                self._send_dm_with_fallback, embed=embed
            ),
        )

    async def notify_judges_of_new_evidence(self, case_id: str, user_id: int) -> None:
        case = await self.repo.get_case_data(case_id)
        if not case:
            logger.warning("Case %s not found for evidence notification", case_id)
            return

        embed = await self.view.generate_embed(
            title="Evidence Submission",
            description=(
                f"New evidence has been submitted for **Case {case_id}**. Please review and decide whether to make it public."
            ),
        )
        buttons = self.view.create_evidence_action_buttons(case_id, user_id)

        await self._notify_users(
            user_ids=frozenset(case.judges),
            notification_func=functools.partial(
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
                "Judge role with ID %s not found in guild %s",
                self.config.JUDGE_ROLE_ID,
                guild_id,
            )
            return

        judge_members: frozenset[int] = frozenset(
            member.id for member in guild.members if judge_role in member.roles
        )
        if not judge_members:
            logger.warning(
                "No judges found with role ID %s for case %s",
                self.config.JUDGE_ROLE_ID,
                case_id,
            )
            return
        case = await self.repo.get_case_data(case_id)
        if not case or not case.thread_id:
            logger.warning(
                "Case %s does not have an associated thread for notifications", case_id
            )
            return

        thread_link = f"https://discord.com/channels/{guild_id}/{case.thread_id}"
        embed = await self.view.generate_embed(
            title=f"Case #{case_id} Complaint Received",
            description=(
                f"Please proceed to the [mediation room]({thread_link}) to participate in mediation."
            ),
        )
        notify_judges_button = interactions.Button(
            custom_id=f"register_{case_id}",
            style=interactions.ButtonStyle.PRIMARY,
            label="Join Mediation",
        )
        components = [interactions.ActionRow(notify_judges_button)]

        await self._notify_users(
            user_ids=judge_members,
            notification_func=functools.partial(
                self._send_dm_with_fallback, embed=embed, components=components
            ),
        )

    async def _send_dm_with_fallback(self, user_id: int, **kwargs: Any) -> None:
        try:
            user = await self.bot.fetch_user(user_id)
            await user.send(**kwargs)
            logger.debug("DM sent to user %s successfully", user_id)
        except HTTPException as http_exc:
            logger.warning(
                "Failed to send DM to user %s: %s",
                user_id,
                http_exc,
                exc_info=True,
            )
        except Exception as exc:
            logger.error(
                "Unexpected error sending DM to user %s: %s",
                user_id,
                exc,
                exc_info=True,
            )

    async def _notify_users(
        self,
        user_ids: frozenset[int],
        notification_func: Callable[[int], Awaitable[None]],
    ) -> None:
        async with asyncio.TaskGroup() as tg:
            tasks = {
                user_id: tg.create_task(
                    self._notify_user_with_handling(user_id, notification_func),
                    name=f"notify_user_{user_id}",
                )
                for user_id in user_ids
            }
            try:
                await next(iter(tasks.values()))
                await asyncio.wait(
                    [task for task in tasks.values()][1:],
                    return_when=asyncio.ALL_COMPLETED,
                )
            except* Exception:
                pass

    @staticmethod
    async def _notify_user_with_handling(
        user_id: int, notification_func: Callable[[int], Awaitable[None]]
    ) -> None:
        try:
            async with asyncio.timeout(5.0):
                await notification_func(user_id)
                logger.debug("Notification sent to user %s", user_id)
        except asyncio.TimeoutError:
            logger.warning("Notification timed out for user %s", user_id)
        except HTTPException as http_exc:
            logger.error(
                "HTTP error notifying user %s: %s",
                user_id,
                http_exc,
                exc_info=True,
            )
        except Exception as exc:
            logger.error(
                "Unexpected error notifying user %s: %s",
                user_id,
                exc,
                exc_info=True,
            )

    #  Role functions

    @manage_user_role.autocomplete("role")
    async def role_autocomplete(
        self, ctx: interactions.AutocompleteContext
    ) -> List[interactions.Choice]:
        focused_option = ctx.focused.lower()
        max_suggestions = 25

        return [
            interactions.Choice(name=role.value, value=role.name)
            for role in CaseRole
            if focused_option in role.value.lower()
        ][:max_suggestions]

    async def manage_case_role(
        self,
        ctx: interactions.SlashContext,
        case_id: str,
        action: RoleAction,
        role: str,
        user: interactions.User,
    ) -> None:
        async def validate_and_get_case_data() -> Data:
            fetched_case_data = await self.repo.get_case_data(case_id)
            if not fetched_case_data:
                raise ValueError(f"Case `{case_id}` not found.")
            return fetched_case_data

        async def validate_user_permissions() -> None:
            if not await self.is_user_assigned_to_case(
                ctx.author.id, case_id, "judges"
            ):
                raise PermissionError("Insufficient permissions to manage case roles.")

        def get_valid_role() -> CaseRole:
            try:
                return CaseRole[role.upper()]
            except KeyError:
                raise ValueError(f"Invalid role: `{role}`.")

        def assign_or_revoke_role(
            roles_dictionary: dict[str, set[int]], role_to_manage: CaseRole
        ) -> tuple[str, str]:
            users_with_role = roles_dictionary.setdefault(role_to_manage.name, set())

            match action:
                case RoleAction.ASSIGN:
                    if user.id in users_with_role:
                        raise ValueError(
                            f"User <@{user.id}> already has the role `{role_to_manage.name}`."
                        )
                    users_with_role.add(user.id)
                    return "Assigned", "to"
                case RoleAction.REVOKE:
                    if user.id not in users_with_role:
                        raise ValueError(
                            f"User <@{user.id}> does not possess the role `{role_to_manage.name}`."
                        )
                    users_with_role.remove(user.id)
                    return "Revoked", "from"
                case _:
                    raise ValueError(f"Invalid action: `{action}`.")

        try:
            async with asyncio.timeout(5.0):
                async with self.case_lock_manager(case_id):
                    fetched_case = await validate_and_get_case_data()
                    await validate_user_permissions()
                    valid_role = get_valid_role()

                    updated_roles = {
                        role: set(users) for role, users in fetched_case.roles.items()
                    }
                    action_str, preposition = assign_or_revoke_role(
                        updated_roles, valid_role
                    )

                    await self.repo.update_case(case_id, roles=updated_roles)

                    notification_message = (
                        f"<@{ctx.author.id}> has {action_str.lower()} the role of "
                        f"`{valid_role.name}` {preposition} <@{user.id}>."
                    )

                    success_message = (
                        f"Successfully {action_str.lower()} role `{valid_role.name}` "
                        f"{preposition} <@{user.id}>."
                    )

                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self.view.send_success(ctx, success_message))
                        tg.create_task(
                            self.notify_case_participants(case_id, notification_message)
                        )

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
        if not (url := attachment.get("url")) or not (
            filename := attachment.get("filename")
        ):
            logger.error(
                "Invalid attachment data: missing required fields",
                extra={"attachment": attachment},
            )
            return None

        try:
            async with asyncio.timeout(10):
                async with aiohttp.ClientSession(
                    timeout=aiohttp.aiohttp.ClientTimeout(total=10),
                    raise_for_status=True,
                ) as session:
                    async with session.get(url) as response:
                        content = await response.read()
                        return interactions.File(
                            file=io.BytesIO(content),
                            file_name=filename,
                            description=f"Evidence attachment for {filename}",
                        )
        except* asyncio.TimeoutError:
            logger.error(
                "Timeout fetching attachment",
                extra={"url": url, "filename": filename},
            )
        except* aiohttp.ClientError:
            logger.error(
                "Network error fetching attachment",
                extra={"url": url, "filename": filename},
                exc_info=True,
            )
        except* OSError as eg:
            logger.error(
                "OS error fetching attachment",
                extra={"url": url, "filename": filename, "error": str(eg)},
            )
        except* ValueError as eg:
            logger.error(
                "Value error fetching attachment",
                extra={"url": url, "filename": filename, "error": str(eg)},
            )
        except* Exception as eg:
            logger.exception(
                "Unexpected error fetching attachment",
                extra={"url": url, "filename": filename, "error": str(eg)},
            )
        return None

    async def process_judge_evidence(self, ctx: interactions.ComponentContext) -> None:
        pattern = re.compile(
            r"^(?P<action>approve|reject)_evidence_(?P<case_id>[a-f0-9]{12})_(?P<user_id>\d+)$"
        )
        if not (match := pattern.match(ctx.custom_id)):
            logger.warning(
                "Invalid evidence action format", extra={"custom_id": ctx.custom_id}
            )
            await self.view.send_error(ctx, "Invalid evidence action format")
            return

        try:
            user_id = int(match["user_id"])
            await self.process_judge_evidence_decision(
                ctx=ctx,
                action=EvidenceAction[match["action"].upper()],
                case_id=match["case_id"],
                user_id=user_id,
            )
        except ValueError:
            logger.warning(
                "Invalid user ID in evidence action",
                extra={"user_id": match["user_id"], "custom_id": ctx.custom_id},
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
            if not case:
                logger.error("Case not found", extra={"case_id": case_id})
                await self.view.send_error(ctx, "Case not found")
                return

            if not (
                evidence := next(
                    (e for e in case.evidence_queue if e.user_id == user_id), None
                )
            ):
                logger.error(
                    "Evidence not found",
                    extra={"case_id": case_id, "user_id": user_id},
                )
                await self.view.send_error(ctx, "Evidence not found")
                return

            evidence.state = action

            evidence_list_attr = f"evidence_{action.name.lower()}"
            evidence_list = getattr(case, evidence_list_attr, [])
            evidence_list.append(evidence)
            case.evidence_queue = [
                e for e in case.evidence_queue if e.user_id != user_id
            ]

            update_success = True
            try:
                async with asyncio.timeout(5.0):
                    await self.repo.update_case(
                        case_id,
                        evidence_queue=case.evidence_queue,
                        **{evidence_list_attr: evidence_list},
                    )
            except asyncio.TimeoutError:
                logger.error(
                    "Timeout updating case evidence",
                    extra={"case_id": case_id, "user_id": user_id},
                )
                update_success = False
            except ValueError as e:
                logger.error(
                    "Invalid data updating case evidence",
                    extra={"case_id": case_id, "user_id": user_id, "error": str(e)},
                )
                update_success = False
            except Exception as e:
                logger.exception(
                    "Unexpected error updating case evidence",
                    extra={"case_id": case_id, "user_id": user_id, "error": str(e)},
                )
                update_success = False

            if not update_success:
                await self.view.send_error(ctx, "Failed to update evidence state")
                return

            notification = f"<@{user_id}>'s evidence has been {action.name.lower()} by <@{ctx.author.id}>."

            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    self.notify_evidence_decision(user_id, case_id, action.name.lower())
                )
                tg.create_task(self.notify_case_participants(case_id, notification))
                tg.create_task(
                    self._handle_evidence_finalization(ctx, case, evidence, action)
                )
                tg.create_task(
                    self.view.send_success(
                        ctx, f"Evidence {action.name.lower()} successfully"
                    )
                )

    async def _handle_evidence_finalization(
        self,
        ctx: interactions.ComponentContext,
        case: Data,
        evidence: Evidence,
        new_state: EvidenceAction,
    ) -> None:
        match new_state:
            case EvidenceAction.APPROVED:
                await self.publish_approved_evidence(ctx, case, evidence)
            case EvidenceAction.REJECTED:
                await self.handle_rejected_evidence(ctx, case, evidence)

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
                                    {"url": att.url, "filename": att.filename}
                                )
                            )
                            for att in evidence.attachments
                        ]
                        if file is not None
                    ]

                await trial_thread.send(content=content, files=files)
                await self.view.send_success(ctx, "Evidence published to trial thread")

        except* HTTPException as eg:
            logger.error(
                "HTTP error publishing evidence",
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
                    tg.create_task(user.send("Your evidence has been rejected"))
                    tg.create_task(
                        self.view.send_success(ctx, "Evidence rejection processed")
                    )

        except* HTTPException as eg:
            match eg:
                case HTTPException(status=403):
                    logger.warning(
                        "Cannot DM user - messages disabled",
                        extra={"user_id": evidence.user_id},
                    )
                case _:
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
                if not (
                    case_id := await self.get_user_active_case_number(message.author.id)
                ):
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
            match eg:
                case ValueError():
                    await message.author.send(str(eg))
                    logger.warning(
                        "Invalid evidence submission",
                        extra={"user_id": message.author.id},
                    )
                case HTTPException(status=403):
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
        if not (case_id := self.find_case_number_by_channel_id(message.channel.id)):
            logger.debug(
                "No case for channel",
                extra={"channel_id": message.channel.id},
            )
            return

        try:
            async with asyncio.timeout(5.0):
                if case := await self.repo.get_case_data(case_id):
                    if message.author.id in case.mute_list:
                        await message.delete()
                        logger.info(
                            "Deleted muted user message",
                            extra={
                                "user_id": message.author.id,
                                "channel_id": message.channel.id,
                            },
                        )

        except* Forbidden:
            logger.warning(
                "Cannot delete message - insufficient permissions",
                extra={"channel_id": message.channel.id},
            )
        except* Exception:
            logger.exception(
                "Failed to handle channel message",
                extra={
                    "channel_id": message.channel.id,
                    "message_id": message.id,
                },
            )

    def create_evidence_dict(
        self,
        message: interactions.Message,
        max_file_size: int,
    ) -> Evidence:
        sanitized_content = self.sanitize_user_input(message.content)
        attachments = [
            {
                "url": attachment.url,
                "filename": self.sanitize_user_input(attachment.filename),
                "content_type": attachment.content_type,
            }
            for attachment in message.attachments
            if self.is_valid_attachment(attachment, max_file_size)
        ]

        evidence = Evidence(
            user_id=message.author.id,
            content=sanitized_content,
            attachments=attachments,
            message_id=message.id,
            timestamp=int(message.created_at.timestamp()),
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
                await stack.enter_async_context(self.case_lock_manager(case_id))
                case = await self.repo.get_case_data(case_id)

                if not case:
                    raise ValueError(f"Case {case_id} not found")

                if case.status not in {CaseStatus.FILED, CaseStatus.IN_PROGRESS}:
                    raise ValueError(f"Case {case_id} not accepting evidence")

                updated_queue = [*case.evidence_queue, evidence]
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(
                        self.repo.update_case(
                            case_id,
                            evidence_queue=updated_queue,
                        )
                    )
                    tg.create_task(
                        asyncio.wait_for(
                            self.notify_judges_of_new_evidence(
                                case_id,
                                evidence.user_id,
                            ),
                            timeout=5.0,
                        )
                    )

            except* asyncio.TimeoutError:
                logger.warning(
                    "Judge notification timeout",
                    extra={"case_id": case_id},
                )
            except* Exception:
                logger.critical(
                    "Evidence processing failed",
                    extra={"case_id": case_id},
                    exc_info=True,
                )
                raise

    # Validation functions

    async def user_has_judge_role(
        self,
        user: Union[interactions.Member, interactions.User],
    ) -> bool:
        member = await self._fetch_member(user)
        return self.config.JUDGE_ROLE_ID in member.role_ids

    async def _fetch_member(
        self,
        user: Union[interactions.Member, interactions.User],
    ) -> interactions.Member:
        return (
            user
            if isinstance(user, interactions.Member)
            else await self._fetch_member_from_user(user)
        )

    async def _fetch_member_from_user(
        self,
        user: interactions.User,
    ) -> interactions.Member:
        async with asyncio.TaskGroup() as tg:
            guild_task = tg.create_task(self.bot.fetch_guild(self.config.GUILD_ID))
            guild = await guild_task
            return await guild.fetch_member(user.id)

    async def user_has_permission_for_closure(
        self,
        user_id: int,
        case_id: str,
        member: interactions.Member,
    ) -> bool:
        async with asyncio.TaskGroup() as tg:
            is_assigned_task = tg.create_task(
                self.is_user_assigned_to_case(user_id, case_id, "plaintiff_id")
            )
            is_assigned = await is_assigned_task
            return is_assigned and self.config.JUDGE_ROLE_ID in member.role_ids

    def is_valid_attachment(
        self,
        attachment: interactions.Attachment,
        max_size: int,
    ) -> bool:
        return (
            attachment.content_type in self.config.ALLOWED_MIME_TYPES
            and attachment.size <= max_size
            and bool(attachment.filename)
            and attachment.height is not None
            if attachment.content_type.startswith("image/")
            else True
        )

    def get_role_check(
        self,
        role: str,
    ) -> Callable[[Data, int], bool]:
        role_checks: Dict[str, Callable[[Data, int], bool]] = {
            "plaintiff_id": lambda case, user_id: case.plaintiff_id == user_id,
            "defendant_id": lambda case, user_id: case.defendant_id == user_id,
            "judges": lambda case, user_id: user_id in case.judges,
            "witnesses": lambda case, user_id: user_id in case.witnesses,
            "attorneys": lambda case, user_id: user_id in case.attorneys,
        }
        return role_checks.get(
            role,
            lambda case, user_id: user_id in getattr(case, role, frozenset()),
        )

    async def is_user_assigned_to_case(
        self,
        user_id: int,
        case_id: str,
        role: str,
    ) -> bool:
        case = await self.repo.get_case_data(case_id)
        if not case:
            logger.error("Case '%s' not found", case_id)
            return False
        role_check = self.get_role_check(role)
        return await asyncio.to_thread(role_check, case, user_id)

    async def has_ongoing_lawsuit(
        self,
        user_id: int,
    ) -> bool:
        return any(
            Data.parse_obj(case).plaintiff_id == user_id
            and Data.parse_obj(case).status != CaseStatus.CLOSED
            for case in self.cached_cases.values()
        )

    # Helper methods

    async def archive_thread(self, thread_id: int) -> bool:
        try:
            async with asyncio.timeout(5.0):
                thread = await self.bot.fetch_channel(thread_id)
                if not isinstance(thread, interactions.ThreadChannel):
                    logger.error(
                        "Invalid channel type",
                        extra={"thread_id": thread_id, "type": type(thread).__name__},
                    )
                    return False

                await thread.edit(archived=True, locked=True)
                logger.info(
                    "Thread archived successfully",
                    extra={"thread_id": thread_id},
                )
                return True

        except* (asyncio.TimeoutError, HTTPException) as eg:
            logger.error(
                "Failed to archive thread",
                extra={"thread_id": thread_id, "error": str(eg)},
            )
        except* Exception:
            logger.exception(
                "Unexpected error archiving thread",
                extra={"thread_id": thread_id},
            )
        return False

    @staticmethod
    def sanitize_user_input(text: str, *, max_length: int = 2000) -> str:

        allowed_tags: frozenset[str] = frozenset()
        allowed_attributes: Dict[str, frozenset[str]] = {}
        whitespace_pattern: re.Pattern = re.compile(r"\s+")

        cleaner = functools.partial(
            bleach.clean,
            tags=allowed_tags,
            attributes=allowed_attributes,
            strip=True,
            protocols=bleach.ALLOWED_PROTOCOLS,
        )

        return whitespace_pattern.sub(
            " ",
            cleaner(text),
        ).strip()[:max_length]

    async def delete_thread_created_message(
        self,
        channel_id: int,
        thread: interactions.ThreadChannel,
    ) -> bool:
        try:
            async with asyncio.timeout(5.0):
                parent_channel = await self.bot.fetch_channel(channel_id)
                if not isinstance(parent_channel, interactions.GuildText):
                    logger.error(
                        "Invalid parent channel type",
                        extra={
                            "channel_id": channel_id,
                            "type": type(parent_channel).__name__,
                        },
                    )
                    return False

                async for message in parent_channel.history(limit=50):
                    if (
                        message.type == interactions.MessageType.THREAD_CREATED
                        and message.thread_id == thread.id
                    ):
                        await message.delete()
                        logger.info(
                            "Thread creation message deleted",
                            extra={"thread_id": thread.id},
                        )
                        return True

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

    async def initialize_chat_thread(self, ctx: interactions.CommandContext) -> bool:
        try:
            async with asyncio.timeout(10.0):
                channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                if not isinstance(channel, interactions.GuildText):
                    logger.error("Invalid courtroom channel type")
                    return False

                async with asyncio.TaskGroup() as tg:
                    thread_task = tg.create_task(
                        self.get_or_create_chat_thread(channel)
                    )
                    thread = await thread_task
                    embed = await self.create_chat_room_setup_embed(thread)

                try:
                    await self.view.send_success(ctx, embed.description)
                    logger.info("Chat thread initialized successfully")
                    return True
                except NotFound:
                    logger.warning("Interaction expired, attempting fallback")
                    await ctx.send(embed=embed, ephemeral=True)
                    return True

        except* (asyncio.TimeoutError, HTTPException) as eg:
            logger.error(
                "Failed to initialize chat thread",
                extra={"error": str(eg)},
            )
        except* Exception:
            logger.exception("Unexpected error in chat thread initialization")
            try:
                await self.view.send_error(
                    ctx,
                    "Setup failed. Please try again later.",
                )
            except NotFound:
                logger.warning("Failed to send error message - interaction expired")
        return False

    async def setup_mediation_thread(self, case_id: str, case: Data) -> bool:
        try:
            async with asyncio.timeout(15.0):
                channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                guild = await self.bot.fetch_guild(self.config.GUILD_ID)

                if not guild:
                    logger.error(
                        "Guild not found", extra={"guild_id": self.config.GUILD_ID}
                    )
                    return False

                thread = await self.create_mediation_thread(channel, case_id)
                judges = await self.get_judges(guild)

                participants = frozenset(
                    {case.plaintiff_id, case.defendant_id}.union(judges)
                )

                async with asyncio.TaskGroup() as tg:
                    tasks = [
                        tg.create_task(self.send_case_summary(thread, case_id, case)),
                        tg.create_task(
                            self.update_case_with_thread(case_id, thread.id)
                        ),
                        tg.create_task(
                            self.add_members_to_thread(thread, participants)
                        ),
                    ]
                    await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

                logger.info(
                    "Mediation thread setup completed", extra={"case_id": case_id}
                )
                return True

        except Exception as e:
            logger.exception(
                "Mediation thread setup failed", extra={"case_id": case_id}
            )
            raise
        return False

    async def send_case_summary(
        self,
        thread: interactions.GuildText,
        case_id: str,
        case: Data,
    ) -> bool:
        try:
            async with asyncio.timeout(5.0):
                async with asyncio.TaskGroup() as tg:
                    embed_task = tg.create_task(
                        self.view.create_summary_embed(case_id, case)
                    )
                    components = self.view.create_action_buttons(case_id)

                    embed = await embed_task
                    await thread.send(embeds=[embed], components=components)

                    logger.info(
                        "Case summary sent",
                        extra={"case_id": case_id, "thread_id": thread.id},
                    )
                    return True

        except* Exception:
            logger.exception(
                "Failed to send case summary",
                extra={"case_id": case_id, "thread_id": thread.id},
            )
        return False

    async def update_case_with_thread(self, case_id: str, thread_id: int) -> bool:
        try:
            async with asyncio.timeout(5.0):
                await self.repo.update_case(case_id, thread_id=thread_id)
                logger.info(
                    "Case updated with thread",
                    extra={"case_id": case_id, "thread_id": thread_id},
                )
                return True

        except* Exception:
            logger.exception(
                "Failed to update case with thread",
                extra={"case_id": case_id, "thread_id": thread_id},
            )
        return False

    @staticmethod
    def _sanitize_input(text: str, max_length: int = 2000) -> str:
        return text.strip()[:max_length]

    # Initialization methods

    @staticmethod
    async def create_mediation_thread(
        channel: interactions.GuildText,
        case_id: str,
    ) -> interactions.GuildText:
        thread_name: str = f"第{case_id}号调解室"

        try:
            async with asyncio.timeout(5.0):
                thread = await channel.create_thread(
                    name=thread_name,
                    thread_type=interactions.ChannelType.GUILD_PRIVATE_THREAD,
                    reason=f"Mediation thread for case {case_id}",
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
        if not isinstance(length, int) or length <= 0:
            raise ValueError("Length must be a positive integer")
        if length > 32:
            raise ValueError("Length cannot exceed 32 characters")

        return secrets.token_hex(length // 2)

    async def create_new_case(
        self,
        ctx: interactions.ModalContext,
        data: Dict[str, str],
        defendant_id: int,
    ) -> Tuple[Optional[str], Optional[Data]]:
        result: Tuple[Optional[str], Optional[Data]] = (None, None)
        try:
            async with asyncio.timeout(10.0):
                case_id: str = self.create_case_id()
                case: Data = await self.create_case_data(ctx, data, defendant_id)

                await self.repo.persist_case(case_id, case)

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
        sanitized_data = {
            key: self._sanitize_input(value)
            for key, value in data.items()
            if value is not None
        }

        current_time = datetime.now(timezone.utc)

        return Data(
            plaintiff_id=ctx.author.id,
            defendant_id=defendant_id,
            accusation=sanitized_data.get("accusation", ""),
            facts=sanitized_data.get("facts", ""),
            status=CaseStatus.FILED,
            judges=frozenset(),
            created_at=current_time,
            updated_at=current_time,
        )

    @staticmethod
    async def create_new_chat_thread(
        channel: interactions.GuildText,
    ) -> interactions.ThreadChannel:
        thread_name: str = "聊天室"

        try:
            async with asyncio.timeout(5.0):
                thread = await channel.create_thread(
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
            async with asyncio.timeout(5.0):
                guild_id: int = thread.guild.id
                thread_id: int = thread.id
                jump_url: str = f"https://discord.com/channels/{guild_id}/{thread_id}"

                message = f"The lawsuit and appeal buttons have been sent to this channel. The chat room thread is ready: [Jump to Thread]({jump_url})."

                embed = await self.view.generate_embed(
                    title="Success", description=message
                )

                logger.debug(
                    "Created chat room setup embed",
                    extra={"thread_id": thread_id, "jump_url": jump_url},
                )
                return embed

        except* Exception:
            logger.error(
                "Failed to create chat room setup embed",
                extra={"thread_id": thread.id},
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
        visibility = "publicly" if is_public else "privately"
        thread_name = (
            f"Case #{case_id} {'Public' if is_public else 'Private'} Trial Chamber"
        )

        try:
            async with asyncio.timeout(10.0):
                async with asyncio.TaskGroup() as tg:
                    courtroom_channel = await tg.create_task(
                        self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)
                    )
                    allowed_roles = await tg.create_task(
                        self.get_allowed_roles(ctx.guild)
                    )

                    trial_thread = await courtroom_channel.create_thread(
                        name=thread_name,
                        thread_type=thread_type,
                        auto_archive_duration=interactions.AutoArchiveDuration.ONE_WEEK,
                        reason=f"Trial thread for case {case_id}",
                    )

                    case.trial_thread_id = trial_thread.id
                    case.allowed_roles = allowed_roles

                    embed = await tg.create_task(
                        self.view.create_summary_embed(case_id, case)
                    )
                    end_trial_button = self.view.create_end_trial_button(case_id)

                    await trial_thread.send(
                        embeds=[embed],
                        components=[end_trial_button],
                    )

                    if is_public:
                        await self.delete_thread_created_message(
                            self.config.COURTROOM_CHANNEL_ID,
                            trial_thread,
                        )

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

    # Retrieval methods

    @staticmethod
    async def get_allowed_roles(guild: interactions.Guild) -> frozenset[int]:
        allowed_role_names: frozenset[str] = frozenset(
            {
                "Prosecutor",
                "Private Prosecutor",
                "Plaintiff",
                "Defendant",
                "Defense Attorney",
                "Legal Representative",
                "Victim",
                "Witness",
            }
        )

        allowed_roles = frozenset(
            role.id for role in guild.roles if role.name in allowed_role_names
        )

        logger.debug(
            "Retrieved allowed roles",
            extra={"role_count": len(allowed_roles), "guild_id": guild.id},
        )
        return allowed_roles

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
                member.id
                for member in judge_role.members
                if self.config.JUDGE_ROLE_ID in member.role_ids
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
        channel = await self.bot.fetch_channel(self.config.COURTROOM_CHANNEL_ID)

        if not isinstance(channel, interactions.GuildText):
            error_msg = f"Invalid channel type: {type(channel).__name__}"
            logger.error(error_msg, extra={"channel_id": channel.id})
            raise ValueError(error_msg)

        logger.debug("Courtroom channel retrieved", extra={"channel_id": channel.id})
        return channel

    async def get_or_create_chat_thread(
        self, channel: interactions.GuildText
    ) -> interactions.ThreadChannel:
        async with asyncio.timeout(5.0):
            existing_thread = await self.find_existing_chat_thread(channel)
            if existing_thread:
                logger.debug(
                    "Retrieved existing chat thread",
                    extra={"thread_id": existing_thread.id},
                )
                return existing_thread

            thread = await self.create_new_chat_thread(channel)
            logger.debug("Created new chat thread", extra={"thread_id": thread.id})
            return thread

    @staticmethod
    async def find_existing_chat_thread(
        channel: interactions.GuildText,
    ) -> Optional[interactions.ThreadChannel]:
        active_threads = await channel.fetch_active_threads()
        thread = next((t for t in active_threads.threads if t.name == "聊天室"), None)

        if thread:
            logger.debug("Found existing chat thread", extra={"thread_id": thread.id})
        return thread

    def case_data_cache(self) -> cachetools.TTLCache[str, Optional[Data]]:
        return cachetools.TTLCache(maxsize=1024, ttl=60)

    async def get_case(self, case_id: str) -> Optional[Data]:
        if case_id not in self.case_data_cache:
            case = await self.repo.get_case_data(case_id)
            self.case_data_cache[case_id] = case
            logger.debug("Case retrieved from repository", extra={"case_id": case_id})
        else:
            case = self.case_data_cache[case_id]
            logger.debug("Case retrieved from cache", extra={"case_id": case_id})
        return case

    async def get_user_active_case_number(self, user_id: int) -> Optional[str]:
        for case_id, case_data in self.cached_cases.items():
            case = Data.parse_obj(case_data)
            if (
                user_id in {case.plaintiff_id, case.defendant_id}
                and case.status != CaseStatus.CLOSED
            ):
                logger.debug(
                    "Found active case for user",
                    extra={"user_id": user_id, "case_id": case_id},
                )
                return case_id

        logger.debug("No active case found for user", extra={"user_id": user_id})
        return None

    def find_case_number_by_channel_id(self, channel_id: int) -> Optional[str]:
        for case_id, case_data in self.cached_cases.items():
            case = Data.parse_obj(case_data)
            if channel_id in {case.thread_id, case.trial_thread_id}:
                logger.debug(
                    "Found case for channel",
                    extra={"channel_id": channel_id, "case_id": case_id},
                )
                return case_id

        logger.debug("No case found for channel", extra={"channel_id": channel_id})
        return None

    # Synchronization functions

    @contextlib.asynccontextmanager
    async def case_lock_manager(self, case_id: str) -> AsyncGenerator[None, None]:
        lock = await self.get_case_lock(case_id)
        start_time = time.perf_counter_ns()
        lock_acquired = False

        try:
            for attempt in range(3):
                try:
                    async with asyncio.timeout(10.0):
                        await lock.acquire()
                        lock_acquired = True
                        break
                except asyncio.TimeoutError:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1 << attempt)
                    continue

            self._lock_timestamps[case_id] = asyncio.get_running_loop().time()

            if case_id in self._lock_contention:
                self._lock_contention[case_id] += 1
            else:
                self._lock_contention[case_id] = 1

            logger.debug(
                "Lock acquired",
                extra={
                    "case_id": case_id,
                    "contention_count": self._lock_contention[case_id],
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
                duration = (time.perf_counter_ns() - start_time) / 1e9
                logger.info(
                    "Lock released",
                    extra={
                        "case_id": case_id,
                        "duration_seconds": f"{duration:.9f}",
                        "contention_count": self._lock_contention[case_id],
                    },
                )

    async def get_case_lock(self, case_id: str) -> asyncio.Lock:
        try:
            if existing_lock := self.case_locks.get(case_id):
                logger.debug("Retrieved existing lock", extra={"case_id": case_id})
                return existing_lock

            lock = asyncio.Lock()
            self.case_locks[case_id] = lock
            logger.debug("Created new lock", extra={"case_id": case_id})
            return lock

        except Exception:
            logger.exception("Lock creation failed", extra={"case_id": case_id})
            raise

    # Transcript utilities

    async def enqueue_case_file_save(self, case_id: str, case: Data) -> None:
        async def save_case_file() -> None:
            try:

                log_channel = await self.bot.fetch_channel(self.config.LOG_CHANNEL_ID)
                log_forum = await self.bot.fetch_channel(self.config.LOG_FORUM_ID)

                if not isinstance(
                    log_channel, interactions.TextChannel
                ) or not isinstance(log_forum, interactions.TextChannel):
                    raise RuntimeError("Failed to fetch required channels")

                log_post = await log_forum.fetch_message(self.config.LOG_POST_ID)
                file_name = f"case_{case_id}_{int(time.time())}.txt"
                content = await self.generate_case_transcript(case_id, case)

                async with contextlib.AsyncExitStack() as stack:
                    temp_file = await stack.enter_async_context(
                        aiofiles.tempfile.NamedTemporaryFile(
                            mode="w+", delete=False, buffering=io.DEFAULT_BUFFER_SIZE
                        )
                    )
                    async_file = await stack.enter_async_context(
                        aiofiles.open(
                            temp_file.name,
                            mode="w",
                            encoding="utf-8",
                            buffering=io.DEFAULT_BUFFER_SIZE,
                        )
                    )

                    await async_file.write(content)
                    await async_file.flush()
                    os.fsync(async_file.fileno())

                    file_obj = interactions.File(temp_file.name, file_name)
                    message = f"Case {case_id} details saved at {datetime.now(timezone.utc).isoformat()}"

                    async with asyncio.timeout(10):
                        send_log = log_channel.send(message, files=[file_obj])
                        send_post = log_post.send(message, files=[file_obj])

                        await send_log
                        await send_post

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
                        break
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
            if not channel_id:
                return None
            channel = await self.bot.fetch_channel(channel_id)
            return await self.generate_thread_transcript(channel)

        async with asyncio.TaskGroup() as tg:
            thread_task = tg.create_task(fetch_channel_transcript(case.thread_id))
            trial_task = tg.create_task(fetch_channel_transcript(case.trial_thread_id))
            transcripts = [t for t in (await thread_task, await trial_task) if t]

        return header + "\n".join(transcripts)

    async def generate_thread_transcript(
        self, thread: Optional[interactions.GuildText]
    ) -> Optional[str]:
        if not thread:
            return None

        try:
            async with asyncio.timeout(30):
                messages = await thread.history(limit=0).flatten()
                formatted_messages: list[Optional[str]] = []

                async with asyncio.TaskGroup() as tg:
                    tasks = [
                        tg.create_task(self.format_message_for_transcript(msg))
                        for msg in reversed(messages)
                    ]
                    formatted_messages.extend([await task for task in tasks])

                return f"\n## {thread.name}\n\n" + "\n".join(
                    msg for msg in formatted_messages if msg is not None
                )

        except (asyncio.TimeoutError, Exception) as e:
            logger.exception(
                "Thread transcript generation failed",
                extra={"thread_id": thread.id, "error": str(e)},
            )
            return None

    @staticmethod
    async def generate_report_header(case_id: str, case: Data, now: datetime) -> str:
        fields = (
            ("Case ID", case_id),
            ("Plaintiff ID", str(case.plaintiff_id)),
            ("Defendant ID", str(case.defendant_id)),
            ("Accusation", case.accusation or ""),
            ("Facts", case.facts or ""),
            ("Filing Time", now.isoformat()),
            (
                "Case Status",
                case.status.name if hasattr(case.status, "name") else str(case.status),
            ),
        )
        return "".join(
            itertools.chain((f"- **{n}:** {v}\n" for n, v in fields), ("\n",))
        )

    @staticmethod
    async def format_message_for_transcript(message: interactions.Message) -> str:
        content_parts: list[str] = []
        content_parts.append(
            f"{message.author} at {message.created_at.isoformat()}: {message.content}"
        )

        if message.attachments:
            content_parts.append(
                "Attachments: "
                + ",".join(map(lambda a: str(a.url), message.attachments))
            )

        if message.edited_timestamp is not None:
            content_parts.append(f"Edited at {message.edited_timestamp.isoformat()}")

        return "".join(map(lambda p: p + "\n", content_parts))
