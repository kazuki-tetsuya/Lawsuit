# Lawsuit

The **Lawsuit** module is designed to manage legal proceedings within a server. It provides structured case management, evidence handling, and role-based access control.

## Features

- Automated case filing system
- Trial thread creation and management
- Case status tracking
- Judge and participant role control
- Permission-based access
- User moderation tools
- Secure evidence submission
- Multi-format file support
- Evidence review process
- Case transcripts and history
- Automated action logging

## Usage

### Slash Commands

- `/lawsuit init`: Initialize the lawsuit management system
  - Requires Judge role
  - Sets up necessary channels and permissions
  - Initializes database and background tasks

- `/lawsuit dispatch`: Deploy the lawsuit interface
  - Requires Judge role
  - Creates lawsuit filing buttons
  - Establishes chat thread for discussions

- `/lawsuit role`: Manage user roles within a case
  - `action` (choice): ASSIGN/REVOKE
  - `role` (choice): Available case roles
  - `user` (user): Target user
  - Requires Judge role in the case

### Context Menus

- Message Context Menu:
  - **Message in Lawsuit**: Manage messages (pin/delete)
  - Available in case threads only
  - Requires Judge role

- User Context Menu:
  - **User in Lawsuit**: Manage user permissions (mute/unmute)
  - Available in case threads only
  - Requires Judge role

### Interactive Components

- File Lawsuit button
- File Appeal button
- Evidence approval/rejection buttons
- Trial visibility controls
- Case management actions (dismiss/accept/withdraw)

## Configuration

### Required IDs

```python
GUILD_ID: int             # Server ID
JUDGE_ROLE_ID: int        # Role ID for judges
PLAINTIFF_ROLE_ID: int    # Role ID for plaintiffs
COURTROOM_CHANNEL_ID: int # Main channel for proceedings
LOG_CHANNEL_ID: int       # Channel for logging
LOG_FORUM_ID: int         # Forum for case archives
LOG_POST_ID: int          # Post for case updates
```

### System Limits

```python
MAX_JUDGES_PER_LAWSUIT: int = 1
MAX_JUDGES_PER_APPEAL: int = 3
MAX_FILE_SIZE: int = 10 * 1024 * 1024  # 10MB
```

### Supported File Types

```python
ALLOWED_MIME_TYPES: Set[str] = {
    "image/jpeg",
    "image/png",
    "application/pdf",
    "text/plain"
}
```
