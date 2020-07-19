import os


def getSetting(fName):
    items = fName.split("-")
    settings = {}
    for s in items:
        setting = s.split("_")
        # print(setting)
        if len(setting) != 2:
            continue
        if setting[1].isnumeric():
            setting[1] = int(setting[1])
        settings[setting[0]] = setting[1]
    return settings


def getRate(setting):
    return setting["txnRate"] * setting["client"]


def sortByRate(data):
    data.sort(key=getRate)


def getData(path, requirement):
    lists = os.listdir(path)
    lists = [x for x in lists if x.endswith("final")]
    data = []
    for f in lists:
        fName = os.path.splitext(f)[0]
        setting = getSetting(fName)
        req = True
        for key in requirement:
            if isinstance(requirement[key], list):
                if setting[key] not in requirement[key]:
                    req = False
                    break
            else:
                if setting[key] != requirement[key]:
                    req = False
                    break
        if not req:
            continue
        setting["fName"] = f
        data.append(setting)
    return data
