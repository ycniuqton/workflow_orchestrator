import re
from functools import wraps
from typing import Callable, Dict, List, Optional, Set

from telebot.util import smart_split
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, MessageHandler, Filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update

from .command_callback import MockingBirdCallBack, ParameterizedCommandCallBack

command_spliter = '|'


class CommandCallBack:
    def __init__(self, is_inline_command=False, command_name=None):
        self._bot = None
        self._message = ''
        self.replier = None
        self.command_name = command_name
        self.command_spliter = command_spliter
        self.is_inline_command = is_inline_command

    def set_inline_command(self):
        self.is_inline_command = True

    def _set_bot(self, bot):
        self._bot = bot
        if self.is_inline_command:
            self.replier = bot.effective_message
            self._message = bot.callback_query.data
            self._message = self._message.replace(self.command_spliter, ' ')
        else:
            self.replier = self._bot.message
            self._message = self._bot.message.text

    def run(self, bot, update):
        self._set_bot(bot)
        self._handle()

    def _handle(self):
        self._send_message(f"OK {self._message}")

    def _send_message(self, message, reply_markup=None, parse_mode=None):
        for text in smart_split(message):
            self.replier.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)

    def _get_message(self):
        return self._message

    def _parse_message_params(self):
        # convert message to list of params
        params = re.findall(r'\S+', self._message)
        if len(params) > 1:
            return params[1:]
        else:
            return None


class CommandGroupHandler(CommandCallBack):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._subcommand_map: Dict[str, Callable] = {}
        self._subcommand_help: Dict[str, str] = {}  # Store help text for each subcommand
        self._command_description: str = ""  # Store main command description
        self.register_sub_command()

    def set_command_description(self, description: str):
        """Set the main command description"""
        self._command_description = description

    def _get_command_name(self) -> str:
        """Get the command name from the first message word"""
        params = re.findall(r'\S+', self._message)
        command = params[0] if params else ""
        command = command[1:] if command.startswith('/') else command
        return command

    def _show_help(self):
        """Show help for the command and its subcommands"""
        command = self.command_name if self.command_name else self._get_command_name()
        message = [f"📖 {self._command_description}\n"]
        message.append("Available subcommands:")

        # Create keyboard with subcommand buttons
        keyboard = []
        row = []
        for subcommand, help_msg in self._subcommand_help.items():
            # Create callback data with command and subcommand
            callback_data = f"{command}{self.command_spliter}{subcommand}"
            # Add button with subcommand name
            row.append(InlineKeyboardButton(subcommand.upper(), callback_data=callback_data))

            # Add help text to message
            message.append(f"• `/{command} {subcommand}` - {help_msg}")

            # Create new row after 2 buttons
            if len(row) == 2:
                keyboard.append(row)
                row = []

        # Add remaining buttons if any
        if row:
            keyboard.append(row)

        # Create reply markup with keyboard
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Send message with buttons
        self._send_message("\n".join(message), reply_markup=reply_markup, parse_mode="Markdown")

    def register_sub_command(self):
        raise NotImplementedError

    def _register_sub_command(self, subcommand: str, handler: Callable, help_text: str):
        """
        Register a handler for a subcommand
        Args:
            subcommand: The subcommand name (e.g., 'show', 'rollback')
            handler: The method to handle this subcommand
            help_text: Help text describing what this subcommand does
        """
        self._subcommand_map[subcommand.lower()] = handler
        self._subcommand_help[subcommand.lower()] = help_text

    def _handle(self):
        """Handle command execution using registered handlers"""
        params = self._parse_message_params()
        if not params:
            self._show_help()
            return

        if params[0].lower() in self._subcommand_map:
            handler = self._subcommand_map[params[0].lower()]
            handler(params[1:] if len(params) > 1 else None)
        else:
            self._show_help()


def check_user_permission(func):
    """Decorator to check if user is allowed to use the bot"""

    @wraps(func)
    def wrapped(self, update, context, *args, **kwargs):
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        # Check user permission
        if self.allowed_users and user_id not in self.allowed_users:
            update.message.reply_text("⛔ Sorry, you are not authorized to use this bot.")
            return

        # Check group permission
        if self.allowed_groups and chat_id not in self.allowed_groups:
            return  # Silently ignore commands from unauthorized groups

        return func(self, update, context, *args, **kwargs)

    return wrapped


class TBot:
    def __init__(self, token, helper_type='command_clicking'):
        """Initialize TBot
        
        Args:
            token: Telegram bot token
            helper_type: Type of helper message format ('command_clicking' or 'markdown')
        """
        self.token = token
        self.helper_type = helper_type
        self.command_handler = {}
        self.command_descriptions = {}
        self.command_categories = {}  # Store command categories
        self.category_icons = {}  # Store category icons
        self.button_callback_handlers = {}  # Store callback handlers for buttons
        self.allowed_users: Set[int] = set()  # Store allowed user IDs
        self.allowed_groups: Set[int] = set()  # Store allowed group IDs
        self.updater = None
        self.dp = None
        self.command_spliter = command_spliter

    def set_allowed_users(self, user_ids: List[int]):
        """Set the list of allowed user IDs
        
        Args:
            user_ids: List of Telegram user IDs that are allowed to use the bot
        """
        self.allowed_users = set(user_ids)

    def set_allowed_groups(self, group_ids: List[int]):
        """Set the list of allowed group IDs
        
        Args:
            group_ids: List of Telegram group IDs where the bot is allowed to respond
        """
        self.allowed_groups = set(group_ids)

    def add_allowed_group(self, group_id: int):
        """Add a single group to allowed groups
        
        Args:
            group_id: Telegram group ID to add
        """
        self.allowed_groups.add(group_id)

    def remove_allowed_group(self, group_id: int):
        """Remove a group from allowed groups
        
        Args:
            group_id: Telegram group ID to remove
        """
        self.allowed_groups.discard(group_id)

    def add_command(self, command, callback, description=None, category=None, update_helper=True):
        """
        Add a command handler
        Args:
            command: Command name without /
            callback: Callback function to handle command
            description: Optional description of what the command does
            category: Optional category for grouping commands
            update_helper: Whether to update help command
        """
        self.command_handler[command] = callback
        if description:
            self.command_descriptions[command] = description
        if category:
            if category not in self.command_categories:
                self.command_categories[category] = []
            self.command_categories[category].append(command)
        if update_helper:
            self._update_helper()

    def register_command(self, command, callback_class: CommandCallBack, description=None, category=None,
                         update_helper=True):
        try:
            self.command_handler[command] = callback_class(command_name=command).run
        except Exception as e:
            print(f"Error registering command {command}: {e}")

        if description:
            self.command_descriptions[command] = description
        if category:
            if category not in self.command_categories:
                self.command_categories[category] = []
            self.command_categories[category].append(command)
        if update_helper:
            self._update_helper()

    def register_command_group(self, command, callback_class: CommandCallBack, description=None, category=None,
                               update_helper=True):
        try:
            self.command_handler[command] = callback_class(command_name=command).run
        except Exception as e:
            print(f"Error registering command {command}: {e}")

        self.register_button_callback(command, callback_class)

        if description:
            self.command_descriptions[command] = description
        if category:
            if category not in self.command_categories:
                self.command_categories[category] = []
            self.command_categories[category].append(command)
        if update_helper:
            self._update_helper()

    def register_button_callback(self, command, handler):
        """Register a handler for button callbacks"""
        self.button_callback_handlers[command] = handler(is_inline_command=True, command_name=command)

    def set_category_icon(self, category: str, icon: str):
        """Set an icon for a command category
        
        Args:
            category: The category name
            icon: The emoji or icon to use for the category
        """
        self.category_icons[category] = icon

    @check_user_permission
    def _handle_button_callback_query(self, update, context):
        """Handle button clicks"""
        query = update.callback_query
        command = query.data.split(self.command_spliter)[0]
        if command in self.button_callback_handlers:
            self.button_callback_handlers[command].run(update, context)
        query.answer()

    @check_user_permission
    def _handle(self, update, context):
        """Handle command execution using registered handlers"""
        command = update.message.text.split()[0][1:]  # Remove the '/' prefix
        if command in self.command_handler:
            self.command_handler[command](update, context)

    def _update_helper(self):
        """Create help command with decorated output"""
        tbot = self  # Keep reference to TBot instance

        def format_help_text():
            if tbot.helper_type == 'command_clicking':
                return format_clickable_help()
            else:
                return format_markup_help()

        def format_markup_help():
            """Format help text as simple markup without buttons"""
            sections = [
                "🤖 *Available Bot Commands*\n"
            ]

            # Group commands by category
            categorized = {}
            uncategorized = []

            commands = set(tbot.command_handler.keys()) - {"help"}
            for cmd in sorted(commands):
                category = None
                for cat, cmds in tbot.command_categories.items():
                    if cmd in cmds:
                        category = cat
                        break

                if category:
                    if category not in categorized:
                        categorized[category] = []
                    desc = tbot.command_descriptions.get(cmd)
                    if desc:
                        categorized[category].append(f"• `/{cmd}` - {desc}")
                    else:
                        categorized[category].append(f"• `/{cmd}`")
                else:
                    desc = tbot.command_descriptions.get(cmd)
                    if desc:
                        uncategorized.append(f"• `/{cmd}` - {desc}")
                    else:
                        uncategorized.append(f"• `/{cmd}`")

            # Add categorized commands
            for category in sorted(categorized.keys()):
                icon = tbot.category_icons.get(category, "")
                sections.append(f"\n{icon} *{category.upper()}*")
                sections.extend(categorized[category])

            # Add uncategorized commands if any
            if uncategorized:
                sections.append("\n📌 *Other Commands*")
                sections.extend(uncategorized)

            return "\n".join(sections)

        def format_clickable_help():
            """Format help text with clickable buttons"""
            # Header with decorative border
            sections = [
                "┏━━━━━━━━━━━━━━━━━━━┓",
                "┃     🤖 Available Bot Commands                    ┃",
                "┗━━━━━━━━━━━━━━━━━━━┛\n"
            ]

            # Group commands by category
            categorized = {}
            uncategorized = []

            commands = set(tbot.command_handler.keys()) - {"help"}
            for cmd in sorted(commands):
                category = None
                for cat, cmds in tbot.command_categories.items():
                    if cmd in cmds:
                        category = cat
                        break

                if category:
                    if category not in categorized:
                        categorized[category] = []
                    desc = tbot.command_descriptions.get(cmd)
                    if desc:
                        categorized[category].append(f"/{cmd}\n  └─ {desc}")
                    else:
                        categorized[category].append(f"/{cmd}")
                else:
                    desc = tbot.command_descriptions.get(cmd)
                    if desc:
                        uncategorized.append(f"/{cmd}\n  └─ {desc}")
                    else:
                        uncategorized.append(f"/{cmd}")

            # Add categorized commands
            for category in sorted(categorized.keys()):
                icon = tbot.category_icons.get(category, "")
                sections.append(f"┌─ {icon} {category.upper()} ")
                sections.append("├────────────────")
                for cmd_text in categorized[category]:
                    sections.append(f"│ {cmd_text}")
                sections.append("└────────────────\n")

            # Add uncategorized commands if any
            if uncategorized:
                sections.append("┌─ 📌 Other Commands ")
                sections.append("├────────────────")
                for cmd_text in uncategorized:
                    sections.append(f"│ {cmd_text}")
                sections.append("└────────────────\n")

            return "\n".join(sections)

        class HelpCallBack(CommandCallBack):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.tbot = tbot  # Store reference to TBot instance

            def _handle(self):
                help_text = format_help_text()
                if self.tbot.helper_type == 'markdown':
                    self._send_message(help_text, parse_mode="Markdown")
                else:
                    self._send_message(help_text)

        self.add_command('help', HelpCallBack().run,
                         description="Show this help message",
                         update_helper=False)

    def _tagged_bot_command_handler(self, update, context):
        if not update.message or not update.message.text:
            return

        text = update.message.text
        if not text.startswith('/'):
            return
        text = text[1:]

        if '@' in text:
            command = text.split('@')[0]
            if command in self.command_handler:
                self.command_handler[command](update, context)

    def listen(self):
        self.updater = Updater(self.token)
        self.dp = self.updater.dispatcher

        for command, callback in self.command_handler.items():
            self.dp.add_handler(CommandHandler(command, self._handle))

        # Add handler for tagged bot commands
        self.dp.add_handler(MessageHandler(Filters.all, self._tagged_bot_command_handler), group=-1)

        # Add handler for button callbacks
        self.dp.add_handler(CallbackQueryHandler(self._handle_button_callback_query))

        self.updater.start_polling()
        self.updater.idle()


if __name__ == "__main__":
    tbot = TBot('7481828748:AAGMjzbbhYzsBzLsroYdra_H4lyvZewMgok')
    tbot.add_command("test", MockingBirdCallBack().run)
    tbot.add_command("parse", ParameterizedCommandCallBack().run)
    tbot.listen()
