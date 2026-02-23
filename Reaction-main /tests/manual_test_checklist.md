# Manual Test Checklist

1) **Private channel/group via invite**
- Start `/report`, choose session mode, and select **Private Channel / Group**.
- Provide a valid invite link (`https://t.me/+<hash>` or `https://t.me/joinchat/<hash>`).
- Confirm the bot reports a successful join and then supply a private message link (`https://t.me/c/<id>/<msg>`).
- Verify target details are shown and callbacks (Back/Cancel/Restart) stay responsive.

2) **Public channel/group**
- Start `/report`, choose **Public Channel / Group**, and send a public message link (`https://t.me/<username>/<msg>`).
- Ensure the bot renders target details (title, username, id, type, member/subscriber count) and proceeds to the reason prompt.

3) **Profile/User target**
- Start `/report`, choose **Story URL (Profile)**, and send a profile username or link.
- Confirm the bot shows user details (name, @username, id, bio/flags when available) and continues to the reporting flow with responsive callbacks.
