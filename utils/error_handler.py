import logging

class ErrorHandler:
    @staticmethod
    def log_stream_error(service, symbol, error):
        """Standardized stream error logging"""
        logging.error(f"❌ {service.upper()} STREAM ERROR [{symbol}]: {error}")
    
    @staticmethod
    def log_api_error(api, symbol, status_code):
        """Standardized API error logging"""
        logging.error(f"❌ {api.upper()} API ERROR [{symbol}]: {status_code}")
    
    @staticmethod
    def log_database_error(operation, symbol, error):
        """Standardized database error logging"""
        logging.error(f"❌ DATABASE ERROR [{operation}] [{symbol}]: {error}")
    
    @staticmethod
    def log_websocket_error(symbol, error):
        """Standardized WebSocket error logging"""
        logging.error(f"❌ WEBSOCKET ERROR [{symbol}]: {error}")
    
    @staticmethod
    def log_prediction_error(symbol, error):
        """Standardized prediction error logging"""
        logging.error(f"❌ PREDICTION ERROR [{symbol}]: {error}")