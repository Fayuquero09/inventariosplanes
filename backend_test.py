import requests
import sys
import json
from datetime import datetime

class RoleBasedAccessTester:
    def __init__(self, base_url="https://auto-connect-62.preview.emergentagent.com"):
        self.base_url = base_url
        self.admin_token = None
        self.group_admin_token = None
        self.admin_cookies = None
        self.group_admin_cookies = None
        self.tests_run = 0
        self.tests_passed = 0
        self.session = requests.Session()

    def run_test(self, name, method, endpoint, expected_status, data=None, use_auth=True):
        """Run a single API test"""
        url = f"{self.base_url}/api/{endpoint}"
        headers = {'Content-Type': 'application/json'}
        
        if use_auth and self.token:
            headers['Authorization'] = f'Bearer {self.token}'

        self.tests_run += 1
        print(f"\n🔍 Testing {name}...")
        print(f"   URL: {method} {url}")
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, cookies=self.cookies)
            elif method == 'POST':
                response = requests.post(url, json=data, headers=headers, cookies=self.cookies)
            elif method == 'PUT':
                response = requests.put(url, json=data, headers=headers, cookies=self.cookies)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, cookies=self.cookies)

            success = response.status_code == expected_status
            if success:
                self.tests_passed += 1
                print(f"✅ Passed - Status: {response.status_code}")
                try:
                    response_data = response.json()
                    if isinstance(response_data, dict) and 'id' in response_data:
                        print(f"   Created ID: {response_data['id']}")
                    return True, response_data
                except:
                    return True, {}
            else:
                print(f"❌ Failed - Expected {expected_status}, got {response.status_code}")
                try:
                    error_data = response.json()
                    print(f"   Error: {error_data}")
                except:
                    print(f"   Response: {response.text}")
                return False, {}

        except Exception as e:
            print(f"❌ Failed - Error: {str(e)}")
            return False, {}

    def test_health_check(self):
        """Test API health"""
        return self.run_test("Health Check", "GET", "health", 200, use_auth=False)

    def test_login(self):
        """Test admin login"""
        success, response = self.run_test(
            "Admin Login",
            "POST",
            "auth/login",
            200,
            data={"email": "admin@autoconnect.com", "password": "Admin123!"},
            use_auth=False
        )
        if success and 'token' in response:
            self.token = response['token']
            print(f"   Token obtained: {self.token[:20]}...")
            return True
        return False

    def test_get_me(self):
        """Test get current user"""
        success, response = self.run_test("Get Current User", "GET", "auth/me", 200)
        if success:
            print(f"   User: {response.get('name')} ({response.get('role')})")
        return success

    def test_create_group(self):
        """Test creating a group"""
        group_data = {
            "name": f"Test Group {datetime.now().strftime('%H%M%S')}",
            "description": "Test automotive group"
        }
        success, response = self.run_test("Create Group", "POST", "groups", 200, group_data)
        if success and 'id' in response:
            self.created_entities['groups'].append(response['id'])
        return success, response

    def test_get_groups(self):
        """Test getting groups"""
        return self.run_test("Get Groups", "GET", "groups", 200)

    def test_create_brand(self, group_id):
        """Test creating a brand"""
        brand_data = {
            "name": f"Test Brand {datetime.now().strftime('%H%M%S')}",
            "group_id": group_id,
            "logo_url": "https://example.com/logo.png"
        }
        success, response = self.run_test("Create Brand", "POST", "brands", 200, brand_data)
        if success and 'id' in response:
            self.created_entities['brands'].append(response['id'])
        return success, response

    def test_get_brands(self):
        """Test getting brands"""
        return self.run_test("Get Brands", "GET", "brands", 200)

    def test_create_agency(self, brand_id):
        """Test creating an agency"""
        agency_data = {
            "name": f"Test Agency {datetime.now().strftime('%H%M%S')}",
            "brand_id": brand_id,
            "address": "123 Test Street",
            "city": "Test City"
        }
        success, response = self.run_test("Create Agency", "POST", "agencies", 200, agency_data)
        if success and 'id' in response:
            self.created_entities['agencies'].append(response['id'])
        return success, response

    def test_get_agencies(self):
        """Test getting agencies"""
        return self.run_test("Get Agencies", "GET", "agencies", 200)

    def test_create_financial_rate(self, group_id):
        """Test creating a financial rate"""
        rate_data = {
            "name": f"Test Rate {datetime.now().strftime('%H%M%S')}",
            "group_id": group_id,
            "annual_rate": 12.5,
            "grace_days": 30
        }
        success, response = self.run_test("Create Financial Rate", "POST", "financial-rates", 200, rate_data)
        if success and 'id' in response:
            self.created_entities['financial_rates'].append(response['id'])
        return success, response

    def test_get_financial_rates(self):
        """Test getting financial rates"""
        return self.run_test("Get Financial Rates", "GET", "financial-rates", 200)

    def test_create_vehicle(self, agency_id):
        """Test creating a vehicle"""
        vehicle_data = {
            "vin": f"TEST{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "model": "Test Model",
            "year": 2024,
            "trim": "Test Trim",
            "color": "Test Color",
            "vehicle_type": "new",
            "purchase_price": 25000.00,
            "agency_id": agency_id
        }
        success, response = self.run_test("Create Vehicle", "POST", "vehicles", 200, vehicle_data)
        if success and 'id' in response:
            self.created_entities['vehicles'].append(response['id'])
        return success, response

    def test_get_vehicles(self):
        """Test getting vehicles"""
        return self.run_test("Get Vehicles", "GET", "vehicles", 200)

    def test_create_sales_objective(self, agency_id):
        """Test creating a sales objective"""
        objective_data = {
            "agency_id": agency_id,
            "month": datetime.now().month,
            "year": datetime.now().year,
            "units_target": 10,
            "revenue_target": 250000.00,
            "vehicle_line": "Test Line"
        }
        success, response = self.run_test("Create Sales Objective", "POST", "sales-objectives", 200, objective_data)
        if success and 'id' in response:
            self.created_entities['sales_objectives'].append(response['id'])
        return success, response

    def test_get_sales_objectives(self):
        """Test getting sales objectives"""
        return self.run_test("Get Sales Objectives", "GET", "sales-objectives", 200)

    def test_create_commission_rule(self, agency_id):
        """Test creating a commission rule"""
        rule_data = {
            "agency_id": agency_id,
            "name": f"Test Rule {datetime.now().strftime('%H%M%S')}",
            "rule_type": "per_unit",
            "value": 500.00,
            "min_units": 1,
            "max_units": 100
        }
        success, response = self.run_test("Create Commission Rule", "POST", "commission-rules", 200, rule_data)
        if success and 'id' in response:
            self.created_entities['commission_rules'].append(response['id'])
        return success, response

    def test_get_commission_rules(self):
        """Test getting commission rules"""
        return self.run_test("Get Commission Rules", "GET", "commission-rules", 200)

    def test_dashboard_kpis(self):
        """Test dashboard KPIs"""
        return self.run_test("Dashboard KPIs", "GET", "dashboard/kpis", 200)

    def test_dashboard_trends(self):
        """Test dashboard trends"""
        return self.run_test("Dashboard Trends", "GET", "dashboard/trends", 200)

    def test_dashboard_suggestions(self):
        """Test dashboard suggestions"""
        return self.run_test("Dashboard Suggestions", "GET", "dashboard/suggestions", 200)

def main():
    print("🚀 Starting AutoConnect API Tests")
    print("=" * 50)
    
    tester = AutoConnectAPITester()
    
    # Test basic connectivity
    if not tester.test_health_check()[0]:
        print("❌ Health check failed, stopping tests")
        return 1

    # Test authentication
    if not tester.test_login():
        print("❌ Login failed, stopping tests")
        return 1

    if not tester.test_get_me():
        print("❌ Get user info failed")
        return 1

    # Test organizational structure creation
    print("\n📋 Testing Organizational Structure...")
    
    # Create Group
    group_success, group_data = tester.test_create_group()
    if not group_success:
        print("❌ Group creation failed, stopping tests")
        return 1
    group_id = group_data['id']

    # Test getting groups
    tester.test_get_groups()

    # Create Brand
    brand_success, brand_data = tester.test_create_brand(group_id)
    if not brand_success:
        print("❌ Brand creation failed, stopping tests")
        return 1
    brand_id = brand_data['id']

    # Test getting brands
    tester.test_get_brands()

    # Create Agency
    agency_success, agency_data = tester.test_create_agency(brand_id)
    if not agency_success:
        print("❌ Agency creation failed, stopping tests")
        return 1
    agency_id = agency_data['id']

    # Test getting agencies
    tester.test_get_agencies()

    # Test Financial Rates
    print("\n💰 Testing Financial Rates...")
    tester.test_create_financial_rate(group_id)
    tester.test_get_financial_rates()

    # Test Vehicles
    print("\n🚗 Testing Vehicles...")
    tester.test_create_vehicle(agency_id)
    tester.test_get_vehicles()

    # Test Sales Objectives
    print("\n🎯 Testing Sales Objectives...")
    tester.test_create_sales_objective(agency_id)
    tester.test_get_sales_objectives()

    # Test Commission Rules
    print("\n💼 Testing Commission Rules...")
    tester.test_create_commission_rule(agency_id)
    tester.test_get_commission_rules()

    # Test Dashboard APIs
    print("\n📊 Testing Dashboard APIs...")
    tester.test_dashboard_kpis()
    tester.test_dashboard_trends()
    tester.test_dashboard_suggestions()

    # Print final results
    print("\n" + "=" * 50)
    print(f"📊 Tests completed: {tester.tests_passed}/{tester.tests_run}")
    
    if tester.tests_passed == tester.tests_run:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"⚠️  {tester.tests_run - tester.tests_passed} tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())