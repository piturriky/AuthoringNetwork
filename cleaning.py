import sys, json, re, io

def JSONValidator( line ):
    try:
        obj = json.loads(line)
        authors = list(set(obj['authors']))
        obj['authors'] = []
        for author in authors:
            obj['authors'].extend(map(lambda x: x.strip().upper(), author.split(';')))
        subjects = list(set(obj['subjects']))
        obj['subjects'] = []
        for subject in subjects:
            obj['subjects'].extend(map(lambda x: x.strip().upper(), subject.split(';')))
        match = re.search('\d{4}', obj['issued'])
        if match:
            obj['issued'] = int(match.group(0))
        else:
            return ''
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ''
   

out = io.open("out.txt", 'w', encoding='utf-8')
with io.open(sys.argv[1], "r", encoding='utf-8') as ins:
    for line in ins:
        line = line[1:-2]
        line = line.replace("\"\"", "\"")
        cleanJson = JSONValidator(line)
        if cleanJson != '':
            out.write(unicode(cleanJson))
            out.write(unicode("\n"))

    ins.close()
out.close()