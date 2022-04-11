from configparser import ConfigParser
from logging import exception


def config(filename="database.ini",section="postgresql",path=".\\.vscode\\"):
    parser = ConfigParser()
    parser.read(path + filename)
    db = {}
    if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                    db[param[0]] = param[1]
    else:
        raise exception('section{0} is not found in the {1} file.'.format(section,filename))
    return db