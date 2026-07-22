# gmail-stats
Generate stats on your GMail inbox and store in a SQLite database

# What?
My GMail inbox was a mess and there's no easy built-in way to view things like who the most frequent senders of mail are.

I found an add-on, [Mailstrom](https://mailstrom.co/) however it was a commercial product and also required granting OAuth
access to manage my entire inbox, which I didn't feel comfortable with.

So, I spent a few hours and hacked this together. The code's not great, it's slow and very basic, but it works.

# How?
First follow the Google Workspace [setup instructions](https://developers.google.com/workspace/guides/get-started) to get OAuth
credentials associated with a Google Cloud Project.

You'll need to make sure your OAuth Consent Screen is configured to have the following scopes:

* `https://www.googleapis.com/auth/gmail.readonly`
* `https://www.googleapis.com/auth/gmail.metadata`

As well as allowing the `redirect_uri` of `http://localhost`.

After setting up your OAuth credentials, download the client secret file and save it as `credentials.json`.

The application creates the local database tables (and a unique index on
`seen_mails.mail_id`, which keeps retries from double-counting messages)
automatically on startup, so no manual schema setup is needed.

Finally, run the application:

```console
$ cargo run
```

## Configuration

Two optional environment variables tune the Gmail API request rate for your
Google Cloud project's per-user quota:

* `GMAIL_STATS_FETCH_CONCURRENCY` — how many `messages.get` calls may be in
  flight at once (default: 8, minimum: 1).
* `GMAIL_STATS_RATE_LIMIT_MS` — minimum spacing between Gmail API calls in
  milliseconds (default: 25, i.e. ~40 requests/sec).

For example, to run gently against a project with a low quota:

```console
$ GMAIL_STATS_FETCH_CONCURRENCY=2 GMAIL_STATS_RATE_LIMIT_MS=100 cargo run
```

This will trigger an OAuth flow which you launch in your browser, after which the access credentials are stored on disk in the local directory.
Please be aware that these are credentials that would allow anyone to read the contents of your email inbox, so you probably want to `rm tokencache.json`
after you're done.

When the script finishes running, you can view the statistics on senders in the DB:

```console
$ sqlite3 stats.db
sqlite> select count(distinct(mail_id)) from seen_mails;
145096
sqlite> select * from senders order by mails_sent asc;
...
```
