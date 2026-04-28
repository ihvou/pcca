# PCCA - Scenarios

This document describes the product-level scenarios PCCA must support.
It is intentionally written from the user and stakeholder perspective.

Implementation details, module interactions, data model choices, reliability controls,
and detailed acceptance criteria belong in [architecture.md](./architecture.md) and
[tasks.md](./tasks.md), not here.

Related documents:
- [architecture.md](./architecture.md) - system design
- [tasks.md](./tasks.md) - implementation backlog
- [CLAUDE.md](./CLAUDE.md) - entry point

---

## Scenario 1: User Installation / Launch / Initial Configuration

### Goal
User installs the app, connects Telegram, connects content sources, creates a first subject, and receives a first useful update.

### Scenario
1. User installs and launches the desktop app.
2. App opens a guided first-run flow.
3. User sets basic local preferences such as timezone and preferred update time.
4. User connects their Telegram bot so the app has a place to send updates.
5. User logs into supported content platforms in their normal browser if needed.
6. User gives the app permission to reuse those local browser sessions for reading followed accounts, channels, feeds, or subscriptions.
7. App imports the sources the user already follows where this is available.
8. User reviews imported sources and removes obvious noise.
9. User confirms which sources the app should monitor.
10. User creates a first subject, such as "Vibe Coding", and describes what is useful or not useful for that subject.
11. App runs an immediate test read and sends a first Telegram update.

---

## Scenario 2: User Starts a New Subject and Sets Preferences

### Goal
User creates a separate area of interest with its own preferences and refinement loop.

### Scenario
1. User describes a new subject in a single free-form Telegram message (text or voice), saying what is useful, what is not, and what good looks like. They can name it or skip naming.
2. Bot proposes a short title (creating one if the user did not), rephrases the description in its own working format, and shows both back for confirmation.
3. User confirms, corrects, or adds detail. The loop repeats as many times as the user wants until the title and rephrasing are right.
4. Bot creates the subject with the agreed-upon title and preferences, and confirms where updates for that subject will appear.
5. Future updates for this subject reflect those preferences.
6. Preferences for one subject do not change another subject unless the user explicitly asks otherwise.

---

## Scenario 3.1: System Collects Fresh Content From Sources

### Goal
The system collects fresh content from monitored sources and stores it so it can be used later by any subject.

### Scenario
1. During the configured overnight window, the system checks sources the user has chosen to monitor.
2. The system collects fresh posts, articles, videos, podcast episodes, newsletter issues, or other supported content.
3. For videos and podcasts, the system gets transcripts when they are available.
4. The system collects as much useful context as possible, such as descriptions, authors, timestamps, views, likes, shares, comments, reposts, and other signals that may help identify trending, breaking, reputable, or debatable content.
5. The system stores collected content and context so they can be reused across subjects.
6. The system avoids storing the same content as new again and again.
7. The system continues with other sources if one source is temporarily unavailable or needs the user to log in again.
8. The system records enough status information for the user to understand which sources were checked and which need attention.

---

## Scenario 3.2: System Finds Subject-Relevant Updates and Builds Output

### Goal
The system finds useful updates for each subject from collected content and prepares the current output format.

### Scenario
1. For each active subject, the system looks through collected content and finds updates relevant to that subject.
2. The same collected content can be useful for more than one subject.
3. The system filters out content that is generic, repetitive, off-topic, or not useful enough for that subject.
4. The system favors updates that are practical, novel, trustworthy, and aligned with the subject's stated preferences.
5. The system prepares an output for the subject without the user doing extra work.
6. For now, the output is a Telegram digest.
7. Later, output may become selectable from templates or fully custom per subject.

---

## Scenario 4: User Gets Content Updates

### Goal
User receives useful updates for each subject at the expected time or on demand.

### Scenario
1. User receives an update in Telegram for each active subject.
2. If there is nothing worth showing, the user gets a short "nothing useful found" message instead of noise.
3. User can request an update manually when they do not want to wait for the scheduled time.
4. User can ask the app to rebuild today's update after adding new sources or changing preferences.

---

## Scenario 5: User Refines Preferences Per Subject

### Goal
User can make the app understand their taste better over time with low friction.

### Scenario
1. User reacts to an update or sends natural-language feedback.
2. User can give feedback by text or voice.
3. Feedback can be broad, such as "less generic AI hype."
4. Feedback can be specific, such as "only include Skills content when there is a new official capability or a genuinely practical workflow."
5. Bot confirms what it understood.
6. Future updates for that subject reflect the refinement.
7. Feedback affects only the current subject unless the user explicitly asks otherwise.
