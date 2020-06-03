#!/usr/bin/env python3
"""Google NLP Analyzer for Reddit Data Gatherer:

This script will take either a single text blob or a json output file from the Reddit
 Data Gatherer and run it through GNLP syntactical analysis.

Output will be either a JSON file, or direct to stdout.

A keyboard interrupt (Ctrl-C) can be used to stop the analysis process and immediately
 write any collected information to disk.
"""

# TODO: import os.path

import ujson as json
import argparse
import signal
import sys
import datetime as dt

# import Google NLP components
from google.cloud import language
from google.cloud.language import enums, types

# Globals
stdoutOnly = False
results = {}
title = ""
file = False


def today():
    return dt.date.today().isoformat()


def writeJson():
    if stdoutOnly:
        print("\n! No JSON file will be written.\n")
        print(json.dumps(results, indent=4), "\n")
    else:
        filename = "./data/{0}_{1}_{2}.json"
        filename = filename.format(
            title if title else "textblob", "syntactical_analysis", today()
        )

        print("\n* Writing to file", filename, end=" ... ", flush=True)
        with open(filename, "w") as fp:
            fp.write(json.dumps(results, indent=4))

        print("Write complete.\n")


def sigintHandler(signal, frame):
    print("\n\n! SIGINT RECEIVED -- bailing")
    if not file:
        writeJson()
    sys.exit(0)


# TODO
def analyze_file(path):
    pass


def analyze_blob(text):
    client = language.LanguageServiceClient()
    document = types.Document(content=text, type=enums.Document.Type.PLAIN_TEXT)

    response = client.analyze_syntax(document=document)

    global results
    results["sentences"] = len(response.sentences)
    results["token_count"] = len(response.tokens)
    results["tokens"] = {}
    tokens = {}

    for token in response.tokens:
        pos = enums.PartOfSpeech.Tag(token.part_of_speech.tag).name
        tok = token.text.content

        if tok not in tokens:
            tokens[tok] = {"part": pos, "count": 1}
        else:
            tokens[tok]["count"] += 1

    sorted_keys = sorted(tokens, key=lambda x: (tokens[x]["count"]), reverse=True)

    for k in sorted_keys:
        results["tokens"][k] = tokens[k]

    writeJson()


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
        help="Specify this option to print gathered data to STDOUT instead of a JSON file.",
    )

    args = parser.parse_args()

    global stdoutOnly
    stdoutOnly = args.stdoutOnly

    global file
    if args.t:
        analyze_blob(args.t.lower())
    elif args.f:
        file = True
        analyze_file(args.f)
    else:
        raise Exception("Something went terribly wrong. So sorry.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigintHandler)
    main()
