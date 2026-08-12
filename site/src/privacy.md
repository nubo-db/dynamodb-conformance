---
layout: layouts/prose.webc
# Hand-authored page: bump when the prose changes so the sitemap stays honest.
lastmod: "2026-07-29"
meta:
  title: Privacy
  description: "What this site collects: cookieless analytics, and nothing stored on your device."
---

# Privacy

This is a static site with no accounts and no forms, so nothing here asks you for your name or your email address. The only thing that happens when you read a page is the analytics below.

## Who's responsible

I'm the data controller for the analytics described below: Martin Hicks, based in Manchester, UK. I operate in accordance with the Data Protection Act 2018 (UK GDPR).

## Analytics

The site uses [Altino](https://altino.dev), a cookieless analytics product, to count visits and see which pages get read. Altino is my data processor for this: it handles the data on my behalf and for no purpose of its own. It sets no cookies and saves nothing on your device, so there's no banner to click.

What gets recorded is the page you visited (the path only, with any query string stripped), the site that referred you (the domain only, and only when it wasn't this site), your browser and device type, and an approximate location.

That location comes from CloudFront, the content delivery network in front of the analytics endpoint. CloudFront works out a rough country, city and latitude/longitude at its nearest edge and passes them along as request headers, which Altino reads. Your IP address is never looked up against a geolocation database.

It isn't stored either. It's combined with your browser's user-agent string, the date and a secret key, then put through a one-way hash, and that result is what counts you as one visitor for the day. The secret key is doing real work there: without it, anyone holding a known address and user agent could compute the same hash themselves and check whether you'd visited. The date goes in too, so the value changes daily and can't be used to follow you from one day to the next. The record holding it deletes itself after 24 hours.

What's kept beyond that is aggregate counts by page, day, country, city, browser and referrer.

If your browser sends a Do Not Track signal, nothing is sent at all.

My legal basis is legitimate interest: knowing which parts of the suite's results people actually use. The data isn't sold, shared or used for advertising.

## Your rights

You can ask what data is held about you, and ask for it to be corrected, deleted, restricted or handed over in a portable format. You can also object to the processing. Email [hello@martinhicks.dev](mailto:hello@martinhicks.dev) for any of those, and if you're unhappy with the answer you can complain to the [Information Commissioner's Office](https://ico.org.uk).

One honest caveat. The analytics keep nothing that ties a record back to you, so in practice there's usually nothing to retrieve or delete. That's a consequence of how it's built rather than a refusal to answer.

## Cookies

This site sets no cookies. The analytics above are cookieless, and there's no consent banner because there's nothing stored on your device to consent to.

## Links elsewhere

Pages here link out to GitHub, to the projects being tested, and to AWS documentation. Those sites have their own privacy notices and their own cookies, and none of that is under this site's control.

## Contact

Questions about any of this: [hello@martinhicks.dev](mailto:hello@martinhicks.dev).
