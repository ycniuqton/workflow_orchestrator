from ._base import CommandCallBack


class MockingBirdCallBack(CommandCallBack):
    def _handle(self):
        self._send_message(self._message)


class ParameterizedCommandCallBack(CommandCallBack):
    def _handle(self):
        self._send_message(str(self._parse_message_params()))


class GetChatInfoCallback(CommandCallBack):
    def _handle(self):
        """Return information about the current chat and topic"""
        chat_id = self._bot.effective_chat.id
        
        info = [
            "📢 Chat Information:",
            f"• Chat ID: `{chat_id}`",
        ]
            
        self._send_message("\n".join(info), parse_mode="Markdown")
