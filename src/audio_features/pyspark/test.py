from datetime import datetime


def get_current_datetime():
    # current date and time
    dt_object = datetime.fromtimestamp(954633600000/1000.0)

    # convert from datetime to timestamp
    return dt_object


print(get_current_datetime())
print("what is wrong?")