"""
Enhanced Accuracy Validation System
"""
import asyncio
import logging
from datetime import datetime, timedelta
from database import db
from multi_asset_support import multi_asset

class AccuracyValidator:
    def __init__(self):
        self.validation_threshold = 0.05  # 5% price difference threshold
    
    async def validate_forecasts(self, symbol, days=7):
        """Validate recent forecasts against actual prices"""
        if not db.pool:
            return {'accuracy': 0, 'validated': 0}
        
        try:
            # Get recent forecasts
            forecasts = await db.get_historical_forecasts(symbol, days)
            validated_count = 0
            correct_predictions = 0
            
            for forecast in forecasts:
                if forecast.get('predicted_price') and not forecast.get('result'):
                    # Get actual price at forecast time
                    try:
                        current_data = multi_asset.get_asset_data(symbol)
                        actual_price = current_data['current_price']
                        predicted_price = float(forecast['predicted_price'])
                        
                        # Calculate accuracy
                        price_diff = abs(actual_price - predicted_price) / predicted_price
                        direction_correct = self._check_direction_accuracy(
                            forecast['forecast_direction'], 
                            current_data['change_24h']
                        )
                        
                        # Determine result
                        if price_diff <= self.validation_threshold and direction_correct:
                            result = 'Hit'
                            correct_predictions += 1
                        else:
                            result = 'Miss'
                        
                        # Store validation result
                        await self._store_validation_result(
                            forecast['id'], 
                            forecast['forecast_direction'],
                            'UP' if current_data['change_24h'] > 0 else 'DOWN',
                            result,
                            (1 - price_diff) * 100 if price_diff <= 1 else 0
                        )
                        
                        validated_count += 1
                        
                    except Exception as e:
                        logging.warning(f"Validation failed for forecast {forecast.get('id')}: {e}")
            
            accuracy = (correct_predictions / validated_count * 100) if validated_count > 0 else 0
            return {'accuracy': round(accuracy, 2), 'validated': validated_count}
            
        except Exception as e:
            logging.error(f"Forecast validation failed for {symbol}: {e}")
            return {'accuracy': 0, 'validated': 0}
    
    def _check_direction_accuracy(self, predicted_direction, actual_change):
        """Check if direction prediction was correct"""
        if predicted_direction == 'UP' and actual_change > 0:
            return True
        elif predicted_direction == 'DOWN' and actual_change < 0:
            return True
        elif predicted_direction == 'HOLD' and abs(actual_change) < 1:
            return True
        return False
    
    async def _store_validation_result(self, forecast_id, forecast_direction, actual_direction, result, accuracy_score):
        """Store validation result in database"""
        if not db.pool:
            return
        
        async with db.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO forecast_accuracy (forecast_id, actual_direction, result, accuracy_score)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (forecast_id) DO UPDATE SET
                    actual_direction = $2,
                    result = $3,
                    accuracy_score = $4,
                    evaluated_at = NOW()
            """, forecast_id, actual_direction, result, accuracy_score)

accuracy_validator = AccuracyValidator()