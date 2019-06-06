import re


def iterable(obj):
    if type(obj) is list:
        return obj
    else:
        return [obj]


def get_output_table(output_table, dbtransformation):
    if output_table is None:
        return generate_table_name(dbtransformation)
    else:
        return output_table


def generate_table_name(dbtransformation):
    predecessor_table = dbtransformation.predecessors[0].output_table
    schema = get_schema(predecessor_table)
    function_name = dbtransformation._callable.__name__
    uid = dbtransformation.id
    context_uid = dbtransformation.context.uuid
    pattern = "{schema}{function_name}_{uid}_{context_uid}"
    table_name = pattern.format(schema=schema,
                                context_uid=context_uid,
                                function_name=function_name,
                                uid=uid)
    return clean_table_name(table_name)


def clean_table_name(string):
    return re.sub('[^\w.-]', '', string)[:63]


def get_schema(output_table):
    if type(output_table) is list:
        table = output_table[0]
    else:
        table = output_table
    split = table.split('.')
    if len(split) < 2:
        return ""
    else:
        return split[0] + "."


def normalize_columns(columns):
    return [normalize_column_name(c) for c in columns]


def normalize_column_name(text):
    """
    Normalizes a column name (removes accents, special characters, etc.)
    :type text: str
    :param text: the text to be normalized
    :return: the normalized text
    """
    import unicodedata, string, re

    # convert to unicode
    if isinstance(text, str):
        try:
            text = text.decode('utf-8')
        except AttributeError:  # Python 3
            pass

    # remove accents
    text_normalized = (unicodedata
                       .normalize('NFKD', text)
                       .encode('ASCII', 'ignore'))

    # lower case
    text_normalized = text_normalized.lower()

    # remove special characters
    allowed_chars = string.digits + string.ascii_letters + ' _'
    text_normalized = u''.join([l for l in text_normalized.decode('utf-8')
                                if l in allowed_chars])
    # strip additional spaces
    text_normalized = text_normalized.strip()
    # replace spaces by _
    text_normalized = re.sub(r' +', '_', text_normalized)

    return text_normalized

