#!/usr/bin/env python3
"""
Async Task Manager for handling concurrent operations
"""
import asyncio
import logging
from typing import Dict, List, Callable, Any
from datetime import datetime

class AsyncTaskManager:
    def __init__(self, max_concurrent_tasks: int = 50):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.task_semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.task_stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'active_tasks': 0
        }
    
    async def run_task(self, task_id: str, coro: Callable, *args, **kwargs) -> Any:
        """Run a task with concurrency control"""
        async with self.task_semaphore:
            self.task_stats['total_tasks'] += 1
            self.task_stats['active_tasks'] += 1
            
            try:
                # Create and store the task
                task = asyncio.create_task(coro(*args, **kwargs))
                self.active_tasks[task_id] = task
                
                # Execute the task
                result = await task
                
                self.task_stats['completed_tasks'] += 1
                return result
                
            except Exception as e:
                self.task_stats['failed_tasks'] += 1
                logging.error(f"Task {task_id} failed: {e}")
                raise
            finally:
                # Cleanup
                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]
                self.task_stats['active_tasks'] -= 1
    
    async def run_background_task(self, task_id: str, coro: Callable, *args, **kwargs):
        """Run a background task without waiting for completion"""
        task = asyncio.create_task(self.run_task(task_id, coro, *args, **kwargs))
        return task
    
    async def run_batch_tasks(self, tasks: List[tuple]) -> List[Any]:
        """Run multiple tasks concurrently"""
        batch_tasks = []
        for task_id, coro, args, kwargs in tasks:
            task = self.run_background_task(task_id, coro, *args, **kwargs)
            batch_tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get task manager statistics"""
        return {
            **self.task_stats,
            'active_task_ids': list(self.active_tasks.keys()),
            'timestamp': datetime.now().isoformat()
        }
    
    async def cleanup_completed_tasks(self):
        """Clean up completed tasks"""
        completed_tasks = []
        for task_id, task in self.active_tasks.items():
            if task.done():
                completed_tasks.append(task_id)
        
        for task_id in completed_tasks:
            del self.active_tasks[task_id]
        
        return len(completed_tasks)

# Global task manager instance
task_manager = AsyncTaskManager(max_concurrent_tasks=100)