import requests
import sys
import json
from datetime import datetime

class HierarchicalFilterTester:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.token = None
        self.tests_run = 0
        self.tests_passed = 0
        self.test_data = {}

    def run_test(self, name, method, endpoint, expected_status, data=None, params=None):
        """Run a single API test"""
        url = f"{self.base_url}/api/{endpoint}"
        headers = {'Content-Type': 'application/json'}
        
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'

        self.tests_run += 1
        print(f"\n🔍 Testing {name}...")
        print(f"   URL: {method} {url}")
        if params:
            print(f"   Params: {params}")
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params)
            elif method == 'POST':
                response = requests.post(url, json=data, headers=headers)

            success = response.status_code == expected_status
            if success:
                self.tests_passed += 1
                print(f"✅ Passed - Status: {response.status_code}")
                try:
                    response_data = response.json()
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

    def setup_test_data(self):
        """Setup test data for hierarchical filtering"""
        print("🏗️  Setting up test data for hierarchical filtering...")
        
        # Login first
        success, response = self.run_test(
            "Admin Login",
            "POST",
            "auth/login",
            200,
            data={"email": "admin@autoconnect.com", "password": "Admin123!"}
        )
        if not success or 'token' not in response:
            print("❌ Login failed, cannot continue")
            return False
        
        self.token = response['token']
        print(f"   Token obtained: {self.token[:20]}...")

        # Create test group
        group_data = {
            "name": f"Filter Test Group {datetime.now().strftime('%H%M%S')}",
            "description": "Test group for hierarchical filtering"
        }
        success, group_response = self.run_test("Create Test Group", "POST", "groups", 200, group_data)
        if not success:
            return False
        self.test_data['group_id'] = group_response['id']
        self.test_data['group_name'] = group_response['name']

        # Create test brand
        brand_data = {
            "name": f"Filter Test Brand {datetime.now().strftime('%H%M%S')}",
            "group_id": self.test_data['group_id'],
            "logo_url": "https://example.com/logo.png"
        }
        success, brand_response = self.run_test("Create Test Brand", "POST", "brands", 200, brand_data)
        if not success:
            return False
        self.test_data['brand_id'] = brand_response['id']
        self.test_data['brand_name'] = brand_response['name']

        # Create test agency
        agency_data = {
            "name": f"Filter Test Agency {datetime.now().strftime('%H%M%S')}",
            "brand_id": self.test_data['brand_id'],
            "address": "123 Filter Test Street",
            "city": "Filter Test City"
        }
        success, agency_response = self.run_test("Create Test Agency", "POST", "agencies", 200, agency_data)
        if not success:
            return False
        self.test_data['agency_id'] = agency_response['id']
        self.test_data['agency_name'] = agency_response['name']

        # Create test seller
        seller_data = {
            "email": f"seller{datetime.now().strftime('%H%M%S')}@test.com",
            "password": "TestPass123!",
            "name": f"Test Seller {datetime.now().strftime('%H%M%S')}",
            "role": "seller",
            "agency_id": self.test_data['agency_id']
        }
        success, seller_response = self.run_test("Create Test Seller", "POST", "auth/register", 200, seller_data)
        if not success:
            return False
        self.test_data['seller_id'] = seller_response['id']
        self.test_data['seller_name'] = seller_response['name']

        # Create test vehicle
        vehicle_data = {
            "vin": f"FILTER{datetime.now().strftime('%Y%m%d%H%M%S')}",
            "model": "Filter Test Model",
            "year": 2026,
            "trim": "Test Trim",
            "color": "Test Color",
            "vehicle_type": "new",
            "purchase_price": 30000.00,
            "agency_id": self.test_data['agency_id']
        }
        success, vehicle_response = self.run_test("Create Test Vehicle", "POST", "vehicles", 200, vehicle_data)
        if not success:
            return False
        self.test_data['vehicle_id'] = vehicle_response['id']

        print(f"✅ Test data setup complete:")
        print(f"   Group: {self.test_data['group_name']} ({self.test_data['group_id']})")
        print(f"   Brand: {self.test_data['brand_name']} ({self.test_data['brand_id']})")
        print(f"   Agency: {self.test_data['agency_name']} ({self.test_data['agency_id']})")
        print(f"   Seller: {self.test_data['seller_name']} ({self.test_data['seller_id']})")
        
        return True

    def test_hierarchical_brands_filter(self):
        """Test brands filtered by group"""
        print("\n🏢 Testing hierarchical brand filtering...")
        
        # Test getting brands for specific group
        success, response = self.run_test(
            "Get Brands by Group",
            "GET",
            "brands",
            200,
            params={"group_id": self.test_data['group_id']}
        )
        
        if success:
            brands = response
            found_test_brand = any(b['id'] == self.test_data['brand_id'] for b in brands)
            if found_test_brand:
                print(f"   ✅ Test brand found in group filter results")
            else:
                print(f"   ❌ Test brand NOT found in group filter results")
                return False
        
        return success

    def test_hierarchical_agencies_filter(self):
        """Test agencies filtered by brand and group"""
        print("\n🏪 Testing hierarchical agency filtering...")
        
        # Test getting agencies for specific brand
        success, response = self.run_test(
            "Get Agencies by Brand",
            "GET",
            "agencies",
            200,
            params={"brand_id": self.test_data['brand_id']}
        )
        
        if success:
            agencies = response
            found_test_agency = any(a['id'] == self.test_data['agency_id'] for a in agencies)
            if found_test_agency:
                print(f"   ✅ Test agency found in brand filter results")
            else:
                print(f"   ❌ Test agency NOT found in brand filter results")
                return False
        
        # Test getting agencies for specific group
        success2, response2 = self.run_test(
            "Get Agencies by Group",
            "GET",
            "agencies",
            200,
            params={"group_id": self.test_data['group_id']}
        )
        
        if success2:
            agencies = response2
            found_test_agency = any(a['id'] == self.test_data['agency_id'] for a in agencies)
            if found_test_agency:
                print(f"   ✅ Test agency found in group filter results")
            else:
                print(f"   ❌ Test agency NOT found in group filter results")
                return False
        
        return success and success2

    def test_hierarchical_sellers_filter(self):
        """Test sellers filtered by agency, brand, and group"""
        print("\n👤 Testing hierarchical seller filtering...")
        
        # Test getting sellers for specific agency
        success, response = self.run_test(
            "Get Sellers by Agency",
            "GET",
            "sellers",
            200,
            params={"agency_id": self.test_data['agency_id']}
        )
        
        if success:
            sellers = response
            found_test_seller = any(s['id'] == self.test_data['seller_id'] for s in sellers)
            if found_test_seller:
                print(f"   ✅ Test seller found in agency filter results")
            else:
                print(f"   ❌ Test seller NOT found in agency filter results")
                return False
        
        return success

    def test_dashboard_kpis_filtering(self):
        """Test dashboard KPIs with hierarchical filtering"""
        print("\n📊 Testing dashboard KPIs with hierarchical filters...")
        
        # Test KPIs with no filter
        success1, response1 = self.run_test(
            "Dashboard KPIs - No Filter",
            "GET",
            "dashboard/kpis",
            200
        )
        
        # Test KPIs with group filter
        success2, response2 = self.run_test(
            "Dashboard KPIs - Group Filter",
            "GET",
            "dashboard/kpis",
            200,
            params={"group_id": self.test_data['group_id']}
        )
        
        # Test KPIs with brand filter
        success3, response3 = self.run_test(
            "Dashboard KPIs - Brand Filter",
            "GET",
            "dashboard/kpis",
            200,
            params={"brand_id": self.test_data['brand_id']}
        )
        
        # Test KPIs with agency filter
        success4, response4 = self.run_test(
            "Dashboard KPIs - Agency Filter",
            "GET",
            "dashboard/kpis",
            200,
            params={"agency_id": self.test_data['agency_id']}
        )
        
        # Test KPIs with seller filter
        success5, response5 = self.run_test(
            "Dashboard KPIs - Seller Filter",
            "GET",
            "dashboard/kpis",
            200,
            params={"seller_id": self.test_data['seller_id']}
        )
        
        if all([success1, success2, success3, success4, success5]):
            print("   ✅ All KPI filter levels working")
            
            # Verify that filtered results are different from unfiltered
            if response1 != response2:
                print("   ✅ Group filter produces different results")
            else:
                print("   ⚠️  Group filter produces same results as no filter")
            
            return True
        
        return False

    def test_dashboard_trends_filtering(self):
        """Test dashboard trends with hierarchical filtering"""
        print("\n📈 Testing dashboard trends with hierarchical filters...")
        
        # Test trends with group filter
        success1, response1 = self.run_test(
            "Dashboard Trends - Group Filter",
            "GET",
            "dashboard/trends",
            200,
            params={"group_id": self.test_data['group_id'], "months": 6}
        )
        
        # Test trends with agency filter
        success2, response2 = self.run_test(
            "Dashboard Trends - Agency Filter",
            "GET",
            "dashboard/trends",
            200,
            params={"agency_id": self.test_data['agency_id'], "months": 6}
        )
        
        # Test trends with seller filter
        success3, response3 = self.run_test(
            "Dashboard Trends - Seller Filter",
            "GET",
            "dashboard/trends",
            200,
            params={"seller_id": self.test_data['seller_id'], "months": 6}
        )
        
        if all([success1, success2, success3]):
            print("   ✅ All trend filter levels working")
            return True
        
        return False

def main():
    print("🚀 Starting Hierarchical Filter Tests")
    print("=" * 60)
    
    tester = HierarchicalFilterTester()
    
    # Setup test data
    if not tester.setup_test_data():
        print("❌ Test data setup failed, stopping tests")
        return 1
    
    # Test hierarchical filtering
    tests = [
        tester.test_hierarchical_brands_filter,
        tester.test_hierarchical_agencies_filter,
        tester.test_hierarchical_sellers_filter,
        tester.test_dashboard_kpis_filtering,
        tester.test_dashboard_trends_filtering
    ]
    
    for test in tests:
        if not test():
            print(f"❌ Test {test.__name__} failed")
    
    # Print final results
    print("\n" + "=" * 60)
    print(f"📊 Hierarchical Filter Tests: {tester.tests_passed}/{tester.tests_run}")
    
    if tester.tests_passed == tester.tests_run:
        print("🎉 All hierarchical filter tests passed!")
        return 0
    else:
        print(f"⚠️  {tester.tests_run - tester.tests_passed} tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
