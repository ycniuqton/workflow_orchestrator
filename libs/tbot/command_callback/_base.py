import re
from typing import Callable, Dict, List, Optional, Set
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update

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
        self.replier.reply_text(message, reply_markup=reply_markup, parse_mode=parse_mode)

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
        command = self._get_command_name()
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
