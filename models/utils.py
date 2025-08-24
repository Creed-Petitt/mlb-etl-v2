def detect_line_movement(old_line, new_line):
    if old_line is None:
        return "new_prop"
    elif new_line > old_line:
        return "line_up"
    elif new_line < old_line:
        return "line_down"
    else:
        return "no_change"