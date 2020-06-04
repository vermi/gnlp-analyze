#!/usr/bin/env python3
"""Google NLP Analyzer for Reddit Data Gatherer:

This script will take either a single text blob or a json output file from the Reddit
 Data Gatherer and run it through GNLP syntactical analysis.

Output will be either a JSON file, or direct to stdout.

A keyboard interrupt (Ctrl-C) can be used to stop the analysis process and immediately
 write any collected information to disk.
"""

# Standard imports
import os.path
import argparse
import signal
import sys
import datetime as dt
from io import StringIO

# Google NLP components
from google.cloud import language
from google.cloud.language import enums, types

# other third-party imports
import ujson as json
from tinydb import TinyDB, Query
from markdown import Markdown
from progress.bar import Bar

# Globals
stdoutOnly = False


def unmark_element(element, stream=None):
    if stream is None:
        stream = StringIO()
    if element.text:
        stream.write(element.text)
    for sub in element:
        unmark_element(sub, stream)
    if element.tail:
        stream.write(element.tail)
    return stream.getvalue()


# patching Markdown
Markdown.output_formats["plain"] = unmark_element
__md = Markdown(output_format="plain")
__md.stripTopLevelTags = False


def unmark(text):
    return __md.convert(text)


def now():
    return dt.datetime.now().isoformat()


def writeJson(output):
    if stdoutOnly:
        print("\n! No JSON file will be written.\n")
        print(json.dumps(output, indent=4), "\n")
    else:
        filename = "./data/{0}_{1}_{2}.json"
        filename = filename.format("textblob", "syntactical_analysis", now())

        print("\n* Writing to file", filename, end=" ... ", flush=True)
        with open(filename, "w") as fp:
            fp.write(json.dumps(output, indent=4))

        print("Write complete.\n")


def sigintHandler(signal, frame):
    print("\n\n! SIGINT RECEIVED -- bailing")
    sys.exit(0)


def gnlp_sentiment(document, isFile=True) -> dict:
    client = language.LanguageServiceClient()

    if not isFile:
        text = document.lower()
    else:
        text = unmark(document["text"]).replace("\n", " ").lower()

    request = types.Document(content=text, type=enums.Document.Type.PLAIN_TEXT)
    response = client.analyze_sentiment(document=request, encoding_type="UTF32")

    sentiment = response.document_sentiment
    data = {
        "score": sentiment.score,
        "magnitude": sentiment.magnitude,
    }

    return data


def gnlp_syntax(document, isFile=True) -> dict:
    client = language.LanguageServiceClient()

    if not isFile:
        text = document.lower()
    else:
        text = unmark(document["text"]).replace("\n", " ").lower()

    request = types.Document(content=text, type=enums.Document.Type.PLAIN_TEXT)
    response = client.analyze_syntax(document=request, encoding_type="UTF32")

    tokens = {}
    sorted_tok = {}

    for token in response.tokens:
        pos = enums.PartOfSpeech.Tag(token.part_of_speech.tag).name
        tok = token.text.content

        if tok not in tokens:
            tokens[tok] = {"part": pos, "count": 1}
        else:
            tokens[tok]["count"] += 1

    sorted_keys = sorted(tokens, key=lambda x: (tokens[x]["count"]), reverse=True)

    for k in sorted_keys:
        sorted_tok[k] = tokens[k]

    data = {
        "sentences": len(response.sentences),
        "token_count": len(response.tokens),
        "tokens": sorted_tok,
    }

    return data


def analyze_file(fpath):
    if not os.path.isfile(fpath):
        print("\n! File {0} not found.".format(fpath))
        exit()

    db = TinyDB(fpath, indent=4)
    if not db.contains(Query().subreddit):
        print("\n! Incorrect database type. Exiting.")
        exit()

    posts_table = db.table("posts")
    comments_table = db.table("comments")

    with Bar("Analyzing corpus", max=len(posts_table) + len(comments_table)) as bar:
        for document in posts_table.all():
            if document["text"] != "":
                syntax = gnlp_syntax(document)
                sentiment = gnlp_sentiment(document)
                posts_table.update(
                    {"syntax": syntax, "sentiment": sentiment},
                    doc_ids=[document.doc_id],
                )

            bar.next()

        for document in comments_table.all():
            if document["text"] != "":
                syntax = gnlp_syntax(document)
                sentiment = gnlp_sentiment(document)
                comments_table.update(
                    {"syntax": syntax, "sentiment": sentiment},
                    doc_ids=[document.doc_id],
                )
            bar.next()


def analyze_blob(text):
    text = text.replace('\\"', '"')

    result = {}
    result["original"] = text
    result["syntax"] = gnlp_syntax(text, isFile=False)
    result["sentiment"] = gnlp_sentiment(text, isFile=False)
    writeJson(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-t", help="A text blob to analyze. Wrap in quotes.", metavar="TEXT"
    )
    group.add_argument(
        "-f", help="Path to properly-formatted JSON file for analysis.", metavar="FILE"
    )

    parser.add_argument(
        "--stdout",
        dest="stdoutOnly",
        action="store_true",
        help="Specify this option to print gathered data to STDOUT instead of a JSON file. Only supports text input.",
    )

    args = parser.parse_args()

    global stdoutOnly
    stdoutOnly = args.stdoutOnly

    if args.t:
        analyze_blob(args.t)
    elif args.f:
        analyze_file(args.f)
    else:
        raise Exception("Something went terribly wrong. So sorry.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigintHandler)
    main()
