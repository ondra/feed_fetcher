#!/usr/bin/env python3
import sys
import os
import re
import logging
import requests
from datetime import datetime
import time
import argparse
import json

import collections
import justext
import lxml

log = logging.getLogger('db')

justext_params = {
    'length_low': 70,            #character count < length_low => bad or short
    'length_high': 140,          #character count > length_high => good
    'stopwords_low': 0.15,       #number of words frequent in the language >= stopwords_low => neargood
    'stopwords_high': 0.3,       #number of words frequent in the language >= stopwords_high => good or neargood
    'max_link_density': 0.4,     #density of link words (words inside the <a> tag) > max_link_density => bad
    'max_good_distance': 5,      #short paragraph block within the distance of # pars from a good par => good
    'max_heading_distance': 500, #short/near-good headings in the distance of # chars before a good par => good
    'no_headings': False,        #keep even short/near-good headings
}

BAD_FILE_EXTENSIONS = 'doc|docx|odt|pdf|ps' \
    '|7z|ai|aiff|apk|asf|avi|bin|bmp|bz2|c|css|deb|djvu|dvi|eot|eps|exe|f4v' \
    '|flv|gif|gz|h|h263|h264|h265|ico|iso|jar|jpg|jpeg|js|m4v|mid|mkv|mng|mov|mp2|mp3|mp4|mpeg' \
    '|mpg|msi|ods|ogg|ogv|pas|phar|png|ppt|pptx|psd|qt|ra|ram|rm|rpm|rtf|sdd|sdw|sh|sit|svg' \
    '|swf|sxc|sxi|sxw|tar|tex|tgz|tif|tiff|ttf|wav|webm|wma|wmf|wmv|woff|xcf|xls|xlsx|xml|xz|zip'
BAD_FILE_EXTENSIONS_RE = re.compile(r'\.(?:%s)$' % BAD_FILE_EXTENSIONS, re.I)

def esc_sa(val):
    if val is None: return '===NONE==='
    return val.replace('\n', ' ').replace('\\', ' ').replace('"', '\\"')

# mimic https://html.spec.whatwg.org/multipage/parsing.html#determining-the-character-encoding
import requests  # for the Content-Type parser
def sniff_encoding(body, content_type):

    # sniff BOM
    if body.startswith(b'\xEF\xBB\xBF'):
        return ('UTF-8', 'BOM')
    if body.startswith(b'\xFE\xFF'):
        return ('UTF-16BE', 'BOM')
    if body.startswith(b'\xFF\xFE'):
        return ('UTF-16LE', 'BOM')

    # check Content-Type transport header
    for ct in content_type:
        mime_essence, opts = requests.utils._parse_content_type_header(ct)
        co = opts.get('charset')
        if co:
            return (co, 'Content-Type')

    # check meta tags
    re_meta1 = re.compile(rb'''<meta\s+http-equiv=['"]?content-type['"]?\s+content=['"]?[^'"]*charset=([^'"]+)''', re.I)
    re_meta2 = re.compile(rb'''<meta\s+content=['"]?[^'"]*charset=([^'"]+)['"]?\s+http-equiv=['"]?content-type['"]?''', re.I)
    re_meta3 = re.compile(rb'''<meta\s+http-equiv=['"]?charset['"]?\s+content=['"]?([^'"]+)''', re.I)
    re_meta4 = re.compile(rb'''<meta\s+content=['"]?([^'"]+)['"]?\s+http-equiv=['"]?charset['"]?''', re.I)
    re_meta5 = re.compile(rb'''<meta\s+charset=['"]?([^'"]+)''', re.I)
    for re_meta in (re_meta1, re_meta2, re_meta3, re_meta4, re_meta5):
        m = re_meta.search(body)
        if m:
            enc = m.group(1).decode('ascii', errors="replace")
            try:
                b' '.decode(enc)
            except LookupError:
                continue
            return (enc, 'meta')

    # give up on sniffing and default to UTF-8
    return ('UTF-8', 'fallback')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wordlist_file", help="justext wordlist file")
    ap.add_argument("--wordlist_lang", help="justext wordlist language")
    ap.add_argument("--no_space_separated_tokens", help="Use Justext settings for scripts not separating tokens by spaces "
        "and no wordlist is supplied to Justext too (useful for languages such as Chinese, Japanese, Korean, Thai)",
        action='store_true')
    ap.add_argument("--badchars", help="documents containing any of the characters will be discarded", default="")
    ap.add_argument("--drop_urls", help="do not dump urls matching those present in this file")
    ap.add_argument("input", default='-')
    args = ap.parse_args()

    if args.wordlist_file:
        with open(args.wordlist_file) as f:
            wordlist = frozenset(line.strip() for line in f.readlines())
    elif args.wordlist_lang:
        wordlist = justext.get_stoplist(args.wordlist_lang)
    elif not args.no_space_separated_tokens:
        log.error("--wordlist_lang or --wordlist_file or --no_space_separated_tokens need to be specified")
        exit(1)

    if args.no_space_separated_tokens:
        #set Justext to use no list of stopwords and ignore the stopword density
        wordlist = set()
        justext_params['stopwords_low'] = 0.0
        justext_params['stopwords_high'] = 0.0
    else:
        assert(len(wordlist))

    bad_urls = frozenset()
    if args.drop_urls is not None:
        with open(args.drop_urls) as f:
            bad_urls = frozenset(l.strip() for l in f.readlines())


    infile = sys.stdin
    if args.input != "-":
        infile = open(args.input)

    log.info("starting page dump")

    npars = 0
    nchars = 0
    nwords = 0
    nvalid = 0
    ntotal = 0
    badenc = 0
    nfixed_date = 0
    nbad_ext = 0
    nbad_urls = 0
    encoding_reason = collections.defaultdict(int)

    for line in infile:
        j = json.loads(line)
        link = j['url']
        if BAD_FILE_EXTENSIONS_RE.search(link):
            nbad_ext += 1
            continue
        if link in bad_urls:
            nbad_urls += 1
            continue
        feed = j.get('feed') or "===NONE==="
        title = j['title']
        published = j['published']
        downloaded = j.get('downloaded') or j['fetched']
        seen = j['seen']
        if 'body_base64' in j:
            import base64
            body = base64.b64decode(j['body_base64'])
            content_type = j['content_type']
            encoding, reason = sniff_encoding(body, content_type)
            encoding_reason[(reason, encoding.lower())] += 1
        else:
            encoding = 'UTF-8'
            body = j['body'].encode('UTF-8')

        try:
            pars = justext.justext(body, wordlist, encoding=encoding, **justext_params)
        except KeyboardInterrupt:
            raise
        except:
            log.error("justext error in " + link, exc_info=1)
            continue
        good_pars = []

        validenc = True
        for par in pars:
            if par['class'] == 'good':
                for badchar in args.badchars:
                    if badchar in par['text']:
                        log.info('likely bad encoding in ' + link)
                        validenc = False
                        break
                else:
                    good_pars.append(par)

        if not validenc:
            badenc += 1
            continue

        #published = "===NONE===" if published is None else published[:10]
        seen = seen[:10]
        downloaded = downloaded[:10]

        #fix feed date to seen date if empty or too far from downloaded date
        from datetime import datetime, timedelta
        downloaded_date = datetime.strptime(downloaded, '%Y-%m-%d')
        date_max = downloaded_date + timedelta(days=1, hours=12)
        date_min = downloaded_date - timedelta(days=730)
        try:
            date_date = datetime.strptime(published[:10], '%Y-%m-%d')
        except:
            date = seen
            nfixed_date += 1
        else:
            if date_date < date_min or date_date > date_max:
                date = seen
                nfixed_date += 1
            else:
                date = datetime.strftime(date_date, '%Y-%m-%d')

        if good_pars:
            print('<doc title="%s" url="%s" feed="%s" date="%s" seen="%s" downloaded="%s">'%
                (esc_sa(title), esc_sa(link), esc_sa(feed), date, seen, downloaded)
                    )
            for par in good_pars:
                npars += 1
                if par['heading']: print('<p heading="1">')
                else: print('<p heading="0">')
                text = par['text']
                nwords += len(text.split()) 
                nchars += len(text) 
                print(text)
                print('</p>')
            print('</doc>')
            nvalid += 1
        else:
            log.info('no useful content in ' + link)

        ntotal += 1

    log.info('done')
    log.info(str(ntotal) + ' documents total')
    log.info(str(nvalid) + ' documents with useful content')
    log.info(str(nchars) + ' valid chars')
    log.info(str(npars) + ' valid pars')
    log.info(str(nwords) + ' valid words')
    log.info(str(badenc) + ' badly encoded docs')
    log.info(str(nbad_ext) + ' skipped urls (bad extension)')
    log.info(str(nbad_urls) + ' skipped urls (bad URL)')
    log.info(str(nfixed_date) + ' feed dates fixed to seen date')
    log.info("encoding decisions made:")
    for k, v in sorted(encoding_reason.items(), key=lambda x: x[1]):
        reason, encoding = k
        log.info(" " + reason + "/" + encoding + ": " + str(v))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)-15s] %(levelname)s: %(message)s")
    exit(main())

