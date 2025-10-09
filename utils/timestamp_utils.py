from datetime import timedelta

class TimestampUtils:
    @staticmethod
    def adjust_for_timeframe(timestamp, timeframe):
        """Adjust timestamp to prevent duplicates for different timeframes"""
        if timeframe == '1W':
            days_since_monday = timestamp.weekday()
            week_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            return week_start - timedelta(days=days_since_monday)
        elif timeframe == '1D':
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == '4H':
            hour_boundary = (timestamp.hour // 4) * 4
            return timestamp.replace(hour=hour_boundary, minute=0, second=0, microsecond=0)
        elif timeframe == '1h':
            return timestamp.replace(minute=0, second=0, microsecond=0)
        elif timeframe in ['15m', '30m']:
            interval = 15 if timeframe == '15m' else 30
            minute_boundary = (timestamp.minute // interval) * interval
            return timestamp.replace(minute=minute_boundary, second=0, microsecond=0)
        elif timeframe == '5m':
            minute_boundary = (timestamp.minute // 5) * 5
            return timestamp.replace(minute=minute_boundary, second=0, microsecond=0)
        else:
            return timestamp.replace(second=0, microsecond=0)