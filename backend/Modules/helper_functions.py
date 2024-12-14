import re


def format_duration(seconds):
    """Format duration from seconds to hours, minutes, and seconds."""
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"


def iso8601_duration_to_seconds(duration):
    """
    Converts an ISO 8601 duration string into the total number of seconds.
    
    Args:
        duration (str): ISO 8601 duration string.
        
    Returns:
        int: Total number of seconds.
    """
    # Regular expression for parsing the ISO 8601 duration format
    pattern = re.compile(
        r'P'                     # starts with 'P'
        r'(?:(\d+)Y)?'           # years
        r'(?:(\d+)M)?'           # months (note: assumes 30 days per month)
        r'(?:(\d+)D)?'           # days
        r'(?:T'                  # time part starts with 'T'
        r'(?:(\d+)H)?'           # hours
        r'(?:(\d+)M)?'           # minutes
        r'(?:(\d+(?:\.\d+)?)S)?)?'  # seconds, capturing group for fractional part
    )
    
    match = pattern.match(duration)
    if not match:
        raise ValueError("Invalid ISO 8601 duration format")

    # Extracting the matched groups, converting None to 0. For seconds, handle potential decimal.
    years, months, days, hours, minutes, seconds = match.groups()
    total_seconds = (
        int(years or 0) * 365 * 24 * 60 * 60 +
        int(months or 0) * 30 * 24 * 60 * 60 +
        int(days or 0) * 24 * 60 * 60 +
        int(hours or 0) * 60 * 60 +
        int(minutes or 0) * 60 +
        int(float(seconds or 0))  # Directly handle conversion from string to float, then to int
    )
    
    return total_seconds


def format_number(num):
    """
    Formats a number using 'K' for thousands, 'M' for millions, and 'B' for billions.
    
    :param num: The number to format.
    :return: A string representing the formatted number.
    """
    if num < 1000:
        return str(num)
    elif num < 1000000:
        return f"{num / 1000:.1f}K"
    elif num < 1000000000:
        return f"{num / 1000000:.1f}M"
    else:
        return f"{num / 1000000000:.1f}B"