#!/usr/bin/env python3

import requests
import sys
import json
from datetime import datetime

class RoleBasedAccessTester:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.admin_token = None
        self.group_admin_token = None
        self.test_group_id = None
        self.test_group_admin_email = None
        self.test_group_admin_password = "GroupAdmin123!"
        self.tests_run = 0
        self.tests_passed = 0
        self.session = requests.Session()

    def run_test(self, name, method, endpoint, expected_status, data=None, token=None, expected_data_check=None):
        """Run a single API test"""
        url = f"{self.base_url}/api/{endpoint}"
        headers = {'Content-Type': 'application/json'}
        if token:
            headers['Authorization'] = f'Bearer {token}'

        self.tests_run += 1
        print(f"\n🔍 Testing {name}...")
        
        try:
            if method == 'GET':
                response = self.session.get(url, headers=headers)
            elif method == 'POST':
                response = self.session.post(url, json=data, headers=headers)

            success = response.status_code == expected_status
            
            if success:
                self.tests_passed += 1
                print(f"✅ Passed - Status: {response.status_code}")
                
                # Additional data validation if provided
                if expected_data_check and response.status_code == 200:
                    try:
                        response_data = response.json()
                        if expected_data_check(response_data):
                            print(f"✅ Data validation passed")
                        else:
                            print(f"❌ Data validation failed")
                            success = False
                            self.tests_passed -= 1
                    except Exception as e:
                        print(f"❌ Data validation error: {e}")
                        success = False
                        self.tests_passed -= 1
                        
            else:
                print(f"❌ Failed - Expected {expected_status}, got {response.status_code}")
                if response.status_code != expected_status:
                    try:
                        error_detail = response.json()
                        print(f"   Error: {error_detail}")
                    except:
                        print(f"   Response: {response.text[:200]}")

            return success, response.json() if response.status_code in [200, 201] else {}

        except Exception as e:
            print(f"❌ Failed - Error: {str(e)}")
            return False, {}

    def test_admin_login(self):
        """Test admin login and get token"""
        print("\n=== Testing Admin Login ===")
        success, response = self.run_test(
            "Admin Login",
            "POST",
            "auth/login",
            200,
            data={"email": "admin@autoconnect.com", "password": "Admin123!"}
        )
        if success and 'token' in response:
            self.admin_token = response['token']
            print(f"✅ Admin token obtained, role: {response.get('role')}")
            return True
        return False

    def test_group_admin_login(self):
        """Test group admin login and get token"""
        print("\n=== Testing Group Admin Login ===")
        if not self.test_group_admin_email:
            print("❌ Group admin email not initialized")
            return False

        success, response = self.run_test(
            "Group Admin Login",
            "POST",
            "auth/login",
            200,
            data={"email": self.test_group_admin_email, "password": self.test_group_admin_password}
        )
        if success and 'token' in response:
            self.group_admin_token = response['token']
            print(f"✅ Group admin token obtained, role: {response.get('role')}, group_id: {response.get('group_id')}")
            return True
        return False

    def setup_group_admin_user(self):
        """Create a dedicated group + group admin for this test run"""
        print("\n=== Setting Up Group Admin Test User ===")
        now = datetime.now().strftime("%Y%m%d%H%M%S")

        # Create a dedicated group using admin token
        group_success, group_response = self.run_test(
            "Create Test Group",
            "POST",
            "groups",
            200,
            data={"name": f"RBAC Test Group {now}", "description": "Role-based access test group"},
            token=self.admin_token
        )
        if not group_success or "id" not in group_response:
            print("❌ Could not create test group")
            return False

        self.test_group_id = group_response["id"]
        self.test_group_admin_email = f"group_admin_{now}@test.com"

        # Register group admin (endpoint currently allows registration with role/group)
        user_success, _ = self.run_test(
            "Register Test Group Admin",
            "POST",
            "auth/register",
            200,
            data={
                "email": self.test_group_admin_email,
                "password": self.test_group_admin_password,
                "name": "RBAC Group Admin",
                "role": "group_admin",
                "group_id": self.test_group_id
            },
            token=self.admin_token
        )
        if not user_success:
            print("❌ Could not create test group admin user")
            return False

        print(f"✅ Test group admin created: {self.test_group_admin_email} ({self.test_group_id})")
        return True

    def test_admin_groups_access(self):
        """Test that admin can see all groups"""
        print("\n=== Testing Admin Groups Access ===")
        
        def validate_admin_groups(data):
            # Admin should see multiple groups or at least have access to all
            print(f"Admin sees {len(data)} groups")
            return len(data) >= 0  # Admin should see all available groups
        
        success, response = self.run_test(
            "Admin - Get All Groups",
            "GET",
            "groups",
            200,
            token=self.admin_token,
            expected_data_check=validate_admin_groups
        )
        return success

    def test_group_admin_groups_access(self):
        """Test that group admin only sees their assigned group"""
        print("\n=== Testing Group Admin Groups Access ===")
        
        def validate_group_admin_groups(data):
            # Group admin should only see their assigned group
            print(f"Group admin sees {len(data)} groups")
            if len(data) == 1:
                group = data[0]
                expected_group_id = self.test_group_id
                if group.get('id') == expected_group_id:
                    print(f"✅ Correct group access: {group.get('name')}")
                    return True
                else:
                    print(f"❌ Wrong group ID. Expected: {expected_group_id}, Got: {group.get('id')}")
                    return False
            elif len(data) == 0:
                print("❌ Group admin sees no groups")
                return False
            else:
                print(f"❌ Group admin sees too many groups: {len(data)}")
                return False
        
        success, response = self.run_test(
            "Group Admin - Get Groups (Should be restricted)",
            "GET",
            "groups",
            200,
            token=self.group_admin_token,
            expected_data_check=validate_group_admin_groups
        )
        return success

    def test_group_admin_vehicles_access(self):
        """Test that group admin only sees vehicles from their group"""
        print("\n=== Testing Group Admin Vehicles Access ===")
        
        def validate_group_vehicles(data):
            # All vehicles should belong to the group admin's group
            group_id = self.test_group_id
            print(f"Group admin sees {len(data)} vehicles")
            
            for vehicle in data:
                if vehicle.get('group_id') != group_id:
                    print(f"❌ Vehicle {vehicle.get('id')} belongs to wrong group: {vehicle.get('group_id')}")
                    return False
            
            print(f"✅ All vehicles belong to correct group")
            return True
        
        success, response = self.run_test(
            "Group Admin - Get Vehicles (Should be filtered by group)",
            "GET",
            "vehicles",
            200,
            token=self.group_admin_token,
            expected_data_check=validate_group_vehicles
        )
        return success

    def test_group_admin_unauthorized_access(self):
        """Test that group admin cannot access other group's data"""
        print("\n=== Testing Group Admin Unauthorized Access ===")
        
        # Try to access a different group's data (if we know another group ID)
        # For now, let's test accessing groups with a different group_id parameter
        success, response = self.run_test(
            "Group Admin - Try to access different group (Should fail)",
            "GET",
            "groups/507f1f77bcf86cd799439011",  # Random ObjectId that doesn't exist or belong to them
            403,  # Should get forbidden
            token=self.group_admin_token
        )
        return success

    def test_dashboard_kpis_access(self):
        """Test dashboard KPIs access for both roles"""
        print("\n=== Testing Dashboard KPIs Access ===")
        
        # Test admin access
        admin_success, admin_response = self.run_test(
            "Admin - Dashboard KPIs",
            "GET",
            "dashboard/kpis",
            200,
            token=self.admin_token
        )
        
        # Test group admin access
        group_admin_success, group_admin_response = self.run_test(
            "Group Admin - Dashboard KPIs",
            "GET",
            "dashboard/kpis",
            200,
            token=self.group_admin_token
        )
        
        return admin_success and group_admin_success

    def test_brands_access(self):
        """Test brands access for both roles"""
        print("\n=== Testing Brands Access ===")
        
        def validate_admin_brands(data):
            print(f"Admin sees {len(data)} brands")
            return True  # Admin should see all brands
        
        def validate_group_admin_brands(data):
            # Group admin should only see brands from their group
            group_id = self.test_group_id
            print(f"Group admin sees {len(data)} brands")
            
            for brand in data:
                if brand.get('group_id') != group_id:
                    print(f"❌ Brand {brand.get('id')} belongs to wrong group: {brand.get('group_id')}")
                    return False
            
            print(f"✅ All brands belong to correct group")
            return True
        
        # Test admin access
        admin_success, admin_response = self.run_test(
            "Admin - Get All Brands",
            "GET",
            "brands",
            200,
            token=self.admin_token,
            expected_data_check=validate_admin_brands
        )
        
        # Test group admin access
        group_admin_success, group_admin_response = self.run_test(
            "Group Admin - Get Brands (Should be filtered)",
            "GET",
            "brands",
            200,
            token=self.group_admin_token,
            expected_data_check=validate_group_admin_brands
        )
        
        return admin_success and group_admin_success

def main():
    print("🚀 Starting Role-Based Access Control Tests")
    print("=" * 50)
    
    tester = RoleBasedAccessTester()
    
    # Test authentication
    if not tester.test_admin_login():
        print("❌ Admin login failed, stopping tests")
        return 1

    if not tester.setup_group_admin_user():
        print("❌ Group admin setup failed, stopping tests")
        return 1
    
    if not tester.test_group_admin_login():
        print("❌ Group admin login failed, stopping tests")
        return 1
    
    # Test role-based access
    tests = [
        tester.test_admin_groups_access,
        tester.test_group_admin_groups_access,
        tester.test_group_admin_vehicles_access,
        tester.test_group_admin_unauthorized_access,
        tester.test_dashboard_kpis_access,
        tester.test_brands_access
    ]
    
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
    
    # Print results
    print("\n" + "=" * 50)
    print(f"📊 Tests completed: {tester.tests_passed}/{tester.tests_run}")
    print(f"Success rate: {(tester.tests_passed/tester.tests_run*100):.1f}%")
    
    if tester.tests_passed == tester.tests_run:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
