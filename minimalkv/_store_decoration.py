from minimalkv.decorator import ReadOnlyDecorator, URLEncodeKeysDecorator


def decorate_store(store, decoratorname):  # noqa D
    decoratorname_part = decoratorname.split("(")[0]
    if decoratorname_part == "urlencode":
        return URLEncodeKeysDecorator(store)
    if decoratorname_part == "readonly":
        return ReadOnlyDecorator(store)
    raise ValueError("Unknown store decorator: " + str(decoratorname))
