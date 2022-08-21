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

Next you'll need to set up the local database tables, as I haven't added SQL migrations yet:

```console
$ sqlite3 stats.db
sqlite> CREATE TABLE seen_mails (mail_id string);
sqlite> CREATE TABLE senders (sender string, mails_sent int);
```

Finally, run the application:

```console
$ cargo run
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
