import re
def get_output(input_str):
    """
    Get output infor from log
    :param input_str:
    :return:
    """
    pattern = '\d{2}[/]\d{2}[/]\d{2} \d{2}[:]\d{2}[:]\d{2}'
    output_str = ""
    if len(input_str) > 1:
        for line in input_str[2:]:
            if not re.match(pattern, line[:17]):
                output_str += line+"\n"

    return output_str