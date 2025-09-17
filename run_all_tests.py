#!/usr/bin/env python3
"""
Test Runner - Execute all API test scripts
Runs comprehensive, quick, and detailed validation tests
"""

import subprocess
import sys
import time
from datetime import datetime

def run_test_script(script_name: str, description: str):
    """Run a test script and return success status"""
    print(f"\n{'='*60}")
    print(f"🚀 Running {description}")
    print(f"Script: {script_name}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        # Run the script
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=False, 
                              text=True, 
                              timeout=300)  # 5 minute timeout
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            print(f"\n✅ {description} completed successfully in {duration:.2f}s")
            return True
        else:
            print(f"\n❌ {description} failed with exit code {result.returncode}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"\n⏰ {description} timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"\n❌ {description} failed with error: {e}")
        return False

def main():
    """Run all test scripts"""
    print("🧪 Trading AI Platform - Complete Test Suite")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nThis will run all API test scripts to validate the backend")
    print("Make sure the server is running at http://localhost:8000\n")
    
    # Test scripts to run
    test_scripts = [
        ("quick_api_test.py", "Quick API Test - Core functionality across all 25 assets"),
        ("detailed_api_validator.py", "Detailed API Validator - Data integrity and structure validation"),
        ("comprehensive_api_test.py", "Comprehensive API Test - Full endpoint coverage")
    ]
    
    results = {}
    total_start_time = time.time()
    
    # Run each test script
    for script, description in test_scripts:
        try:
            success = run_test_script(script, description)
            results[script] = success
        except KeyboardInterrupt:
            print(f"\n⚠️ Test suite interrupted by user")
            break
    
    # Generate summary report
    total_duration = time.time() - total_start_time
    
    print(f"\n{'='*60}")
    print("📊 COMPLETE TEST SUITE SUMMARY")
    print(f"{'='*60}")
    print(f"Total Duration: {total_duration:.2f} seconds")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    successful_tests = sum(1 for success in results.values() if success)
    total_tests = len(results)
    
    print(f"\nTest Scripts Results:")
    for script, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"   {script}: {status}")
    
    print(f"\nOverall Results:")
    print(f"   Successful: {successful_tests}/{total_tests}")
    print(f"   Success Rate: {(successful_tests/total_tests*100):.1f}%")
    
    if successful_tests == total_tests:
        print(f"\n🎉 ALL TESTS PASSED! Your API is working correctly.")
    elif successful_tests >= total_tests * 0.8:
        print(f"\n✅ Most tests passed. Check failed tests for minor issues.")
    else:
        print(f"\n❌ Multiple test failures detected. Review your API implementation.")
    
    print(f"\n📁 Check the following files for detailed results:")
    print(f"   • test_results.json - Comprehensive test results")
    print(f"   • validation_report.json - Detailed validation report")
    
    print(f"{'='*60}")
    
    return 0 if successful_tests == total_tests else 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n👋 Test suite terminated by user")
        sys.exit(1)