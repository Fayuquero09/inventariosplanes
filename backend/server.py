from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, APIRouter, HTTPException, Request, Response, UploadFile, File, Depends, Query
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
import os
import logging
import bcrypt
import jwt
import secrets
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, EmailStr
import pandas as pd
from io import BytesIO
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# JWT Configuration
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours
REFRESH_TOKEN_EXPIRE_DAYS = 7

def get_jwt_secret() -> str:
    return os.environ.get("JWT_SECRET", "default-secret-change-me")

# Password functions
def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))

# JWT functions
def create_access_token(user_id: str, email: str, role: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        "type": "access"
    }
    return jwt.encode(payload, get_jwt_secret(), algorithm=JWT_ALGORITHM)

def create_refresh_token(user_id: str) -> str:
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        "type": "refresh"
    }
    return jwt.encode(payload, get_jwt_secret(), algorithm=JWT_ALGORITHM)

# Create the main app
app = FastAPI(title="AutoConnect - Vehicle Inventory Management")

# Session middleware for OAuth
app.add_middleware(SessionMiddleware, secret_key=get_jwt_secret())

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# ============== PYDANTIC MODELS ==============

class UserRole:
    APP_ADMIN = "app_admin"
    APP_USER = "app_user"
    GROUP_ADMIN = "group_admin"
    BRAND_ADMIN = "brand_admin"
    AGENCY_ADMIN = "agency_admin"
    GROUP_USER = "group_user"
    BRAND_USER = "brand_user"
    AGENCY_USER = "agency_user"
    SELLER = "seller"

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    name: str
    role: str = UserRole.APP_USER
    group_id: Optional[str] = None
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class PasswordResetRequest(BaseModel):
    email: EmailStr
    new_password: str

class UserResponse(BaseModel):
    id: str
    email: str
    name: str
    role: str
    group_id: Optional[str] = None
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    created_at: str

class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = None

class GroupResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    created_at: str

class BrandCreate(BaseModel):
    name: str
    group_id: str
    logo_url: Optional[str] = None

class BrandResponse(BaseModel):
    id: str
    name: str
    group_id: str
    logo_url: Optional[str] = None
    created_at: str

class AgencyCreate(BaseModel):
    name: str
    brand_id: str
    address: Optional[str] = None
    city: Optional[str] = None

class AgencyResponse(BaseModel):
    id: str
    name: str
    brand_id: str
    address: Optional[str] = None
    city: Optional[str] = None
    created_at: str

class FinancialRateCreate(BaseModel):
    group_id: str
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    tiie_rate: float = 11.25  # Tasa TIIE base actual
    spread: float  # Spread adicional %
    grace_days: int = 0  # Días de gracia
    name: str

class FinancialRateResponse(BaseModel):
    id: str
    group_id: str
    brand_id: Optional[str] = None
    agency_id: Optional[str] = None
    tiie_rate: float
    spread: float
    total_rate: float  # tiie_rate + spread
    grace_days: int
    name: str
    created_at: str

class VehicleCreate(BaseModel):
    vin: str
    model: str
    year: int
    trim: str
    color: str
    vehicle_type: str  # new, used
    purchase_price: float
    agency_id: str
    entry_date: Optional[str] = None

class VehicleResponse(BaseModel):
    id: str
    vin: str
    model: str
    year: int
    trim: str
    color: str
    vehicle_type: str
    purchase_price: float
    agency_id: str
    agency_name: Optional[str] = None
    brand_id: Optional[str] = None
    brand_name: Optional[str] = None
    group_id: Optional[str] = None
    entry_date: str
    exit_date: Optional[str] = None
    status: str  # in_stock, sold, transferred
    aging_days: int
    financial_cost: float
    created_at: str

class SalesObjectiveCreate(BaseModel):
    seller_id: Optional[str] = None  # Si es nulo, aplica a toda la agencia
    agency_id: str
    month: int
    year: int
    units_target: int
    revenue_target: float
    vehicle_line: Optional[str] = None

class SalesObjectiveResponse(BaseModel):
    id: str
    seller_id: Optional[str] = None
    seller_name: Optional[str] = None
    agency_id: str
    agency_name: Optional[str] = None
    brand_id: Optional[str] = None
    brand_name: Optional[str] = None
    group_id: Optional[str] = None
    month: int
    year: int
    units_target: int
    revenue_target: float
    vehicle_line: Optional[str] = None
    units_sold: int = 0
    revenue_achieved: float = 0
    progress_units: float = 0
    progress_revenue: float = 0
    created_at: str

class CommissionRuleCreate(BaseModel):
    agency_id: str
    name: str
    rule_type: str  # per_unit, percentage, volume_bonus, fi_bonus
    value: float
    min_units: Optional[int] = None
    max_units: Optional[int] = None

class CommissionRuleResponse(BaseModel):
    id: str
    agency_id: str
    name: str
    rule_type: str
    value: float
    min_units: Optional[int] = None
    max_units: Optional[int] = None
    created_at: str

class SaleCreate(BaseModel):
    vehicle_id: str
    seller_id: str
    sale_price: float
    sale_date: Optional[str] = None
    fi_revenue: float = 0

class SaleResponse(BaseModel):
    id: str
    vehicle_id: str
    vehicle_info: Optional[Dict] = None
    seller_id: str
    seller_name: Optional[str] = None
    agency_id: str
    sale_price: float
    sale_date: str
    fi_revenue: float
    commission: float
    created_at: str

class VehicleSuggestion(BaseModel):
    vehicle_id: str
    vehicle_info: Dict
    avg_days_to_sell: int
    current_aging: int
    financial_cost: float
    suggested_bonus: float
    reason: str

# ============== AUTH HELPER ==============

async def get_current_user(request: Request) -> dict:
    token = request.cookies.get("access_token")
    if not token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(token, get_jwt_secret(), algorithms=[JWT_ALGORITHM])
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
        user = await db.users.find_one({"_id": ObjectId(payload["sub"])})
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        user["id"] = str(user["_id"])
        del user["_id"]
        user.pop("password_hash", None)
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_optional_user(request: Request) -> Optional[dict]:
    try:
        return await get_current_user(request)
    except:
        return None

def serialize_doc(doc: dict) -> dict:
    """Convert MongoDB document to serializable dict"""
    if doc is None:
        return None
    result = {}
    for k, v in doc.items():
        if k == "_id":
            result["id"] = str(v)
        elif isinstance(v, ObjectId):
            result[k] = str(v)
        elif isinstance(v, datetime):
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result

# ============== AUTH ROUTES ==============

@api_router.post("/auth/register")
async def register(user_data: UserCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")

    if current_user["role"] == UserRole.GROUP_ADMIN:
        if user_data.role in [UserRole.APP_ADMIN, UserRole.APP_USER]:
            raise HTTPException(status_code=403, detail="Group admin cannot create app-level users")

        if not current_user.get("group_id"):
            raise HTTPException(status_code=403, detail="Group admin has no assigned group")

        # Group admins can only create users inside their own group.
        if user_data.group_id and user_data.group_id != current_user["group_id"]:
            raise HTTPException(status_code=403, detail="Cannot create users outside your group")
        user_data.group_id = current_user["group_id"]

    email = str(user_data.email).strip().lower()
    existing = await db.users.find_one({"email": email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    if user_data.brand_id:
        brand = await db.brands.find_one({"_id": ObjectId(user_data.brand_id)})
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")
        if user_data.group_id and brand.get("group_id") != user_data.group_id:
            raise HTTPException(status_code=400, detail="Brand does not belong to selected group")
        if not user_data.group_id:
            user_data.group_id = brand.get("group_id")

    if user_data.agency_id:
        agency = await db.agencies.find_one({"_id": ObjectId(user_data.agency_id)})
        if not agency:
            raise HTTPException(status_code=404, detail="Agency not found")
        if user_data.group_id and agency.get("group_id") != user_data.group_id:
            raise HTTPException(status_code=400, detail="Agency does not belong to selected group")
        if user_data.brand_id and agency.get("brand_id") != user_data.brand_id:
            raise HTTPException(status_code=400, detail="Agency does not belong to selected brand")
        if not user_data.group_id:
            user_data.group_id = agency.get("group_id")
        if not user_data.brand_id:
            user_data.brand_id = agency.get("brand_id")

    user_doc = {
        "email": email,
        "password_hash": hash_password(user_data.password),
        "name": user_data.name,
        "role": user_data.role,
        "group_id": user_data.group_id,
        "brand_id": user_data.brand_id,
        "agency_id": user_data.agency_id,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.users.insert_one(user_doc)
    user_id = str(result.inserted_id)

    return {
        "id": user_id,
        "email": email,
        "name": user_data.name,
        "role": user_data.role,
        "group_id": user_data.group_id,
        "brand_id": user_data.brand_id,
        "agency_id": user_data.agency_id,
        "created_at": user_doc["created_at"].isoformat()
    }

@api_router.post("/auth/login")
async def login(user_data: UserLogin, response: Response):
    email = str(user_data.email).strip().lower()
    user = await db.users.find_one({"email": email})
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    password_raw = user_data.password or ""
    password_trimmed = password_raw.strip()
    if not (
        verify_password(password_raw, user["password_hash"]) or
        (password_trimmed != password_raw and verify_password(password_trimmed, user["password_hash"]))
    ):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    user_id = str(user["_id"])
    access_token = create_access_token(user_id, email, user["role"])
    refresh_token = create_refresh_token(user_id)
    
    response.set_cookie(key="access_token", value=access_token, httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
    response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, secure=False, samesite="lax", max_age=604800, path="/")
    
    return {
        "id": user_id,
        "email": user["email"],
        "name": user["name"],
        "role": user["role"],
        "group_id": user.get("group_id"),
        "brand_id": user.get("brand_id"),
        "agency_id": user.get("agency_id"),
        "created_at": user["created_at"].isoformat() if isinstance(user["created_at"], datetime) else user["created_at"],
        "token": access_token
    }

@api_router.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")
    return {"message": "Logged out successfully"}

@api_router.post("/auth/reset-password")
async def reset_password(payload: PasswordResetRequest):
    email = str(payload.email).strip().lower()

    if len(payload.new_password or "") < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    user = await db.users.find_one({"email": email})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"password_hash": hash_password(payload.new_password)}}
    )

    return {"message": "Password updated successfully"}

@api_router.get("/auth/me")
async def get_me(request: Request):
    user = await get_current_user(request)
    return user

@api_router.post("/auth/google")
async def google_auth(request: Request, response: Response):
    """Handle Google OAuth callback"""
    data = await request.json()
    credential = data.get("credential")
    
    if not credential:
        raise HTTPException(status_code=400, detail="No credential provided")
    
    try:
        # Decode the JWT token from Google (without verification for simplicity)
        # In production, verify with Google's public keys
        import base64
        parts = credential.split(".")
        payload = parts[1]
        # Add padding if necessary
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        decoded = base64.urlsafe_b64decode(payload)
        google_user = json.loads(decoded)
        
        email = str(google_user.get("email", "")).strip().lower()
        name = google_user.get("name", "")
        
        # Check if user exists
        user = await db.users.find_one({"email": email})
        
        if not user:
            # Create new user
            user_doc = {
                "email": email,
                "password_hash": "",  # No password for Google users
                "name": name,
                "role": UserRole.APP_USER,
                "group_id": None,
                "brand_id": None,
                "agency_id": None,
                "google_id": google_user.get("sub"),
                "created_at": datetime.now(timezone.utc)
            }
            result = await db.users.insert_one(user_doc)
            user_id = str(result.inserted_id)
            user = user_doc
            user["_id"] = result.inserted_id
        else:
            user_id = str(user["_id"])
        
        access_token = create_access_token(user_id, email, user.get("role", UserRole.APP_USER))
        refresh_token = create_refresh_token(user_id)
        
        response.set_cookie(key="access_token", value=access_token, httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
        response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, secure=False, samesite="lax", max_age=604800, path="/")
        
        return {
            "id": user_id,
            "email": email,
            "name": user.get("name", name),
            "role": user.get("role", UserRole.APP_USER),
            "group_id": user.get("group_id"),
            "brand_id": user.get("brand_id"),
            "agency_id": user.get("agency_id"),
            "created_at": user["created_at"].isoformat() if isinstance(user.get("created_at"), datetime) else str(user.get("created_at", "")),
            "token": access_token
        }
    except Exception as e:
        logger.error(f"Google auth error: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid Google credential: {str(e)}")

# ============== USERS ROUTES ==============

@api_router.get("/users")
async def get_users(request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    query = {}
    if current_user["role"] == UserRole.GROUP_ADMIN and current_user.get("group_id"):
        query["group_id"] = current_user["group_id"]
    
    users = await db.users.find(query, {"password_hash": 0}).to_list(1000)
    return [serialize_doc(u) for u in users]

@api_router.put("/users/{user_id}")
async def update_user(user_id: str, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    data = await request.json()
    update_data = {k: v for k, v in data.items() if k not in ["id", "_id", "password_hash", "email"]}
    
    await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update_data})
    user = await db.users.find_one({"_id": ObjectId(user_id)}, {"password_hash": 0})
    return serialize_doc(user)

@api_router.get("/sellers")
async def get_sellers(request: Request, agency_id: Optional[str] = None, brand_id: Optional[str] = None, group_id: Optional[str] = None):
    """Get sellers (users with seller role) filtered by agency/brand/group"""
    current_user = await get_current_user(request)
    
    query = {"role": UserRole.SELLER}
    
    if agency_id:
        query["agency_id"] = agency_id
    elif brand_id:
        # Get all agencies of this brand
        agencies = await db.agencies.find({"brand_id": brand_id}).to_list(1000)
        agency_ids = [a["_id"] for a in agencies]
        if agency_ids:
            query["agency_id"] = {"$in": [str(a) for a in agency_ids]}
    elif group_id:
        # Get all agencies of this group
        agencies = await db.agencies.find({"group_id": group_id}).to_list(1000)
        agency_ids = [str(a["_id"]) for a in agencies]
        if agency_ids:
            query["agency_id"] = {"$in": agency_ids}
    
    sellers = await db.users.find(query, {"password_hash": 0}).to_list(1000)
    
    # Enrich with agency name
    result = []
    for seller in sellers:
        s = serialize_doc(seller)
        if seller.get("agency_id"):
            agency = await db.agencies.find_one({"_id": ObjectId(seller["agency_id"])})
            if agency:
                s["agency_name"] = agency["name"]
        result.append(s)
    
    return result

# ============== GROUPS ROUTES ==============

@api_router.post("/groups")
async def create_group(group_data: GroupCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] != UserRole.APP_ADMIN:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    group_doc = {
        "name": group_data.name,
        "description": group_data.description,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.groups.insert_one(group_doc)
    group_doc["id"] = str(result.inserted_id)
    return serialize_doc(group_doc)

@api_router.get("/groups")
async def get_groups(request: Request):
    current_user = await get_current_user(request)
    
    query = {}
    # Super admin y super users pueden ver todos los grupos
    # Otros roles solo ven su grupo asignado
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if current_user.get("group_id"):
            query["_id"] = ObjectId(current_user["group_id"])
        else:
            # Si no tiene grupo asignado, no ve ninguno
            return []
    
    groups = await db.groups.find(query).to_list(1000)
    return [serialize_doc(g) for g in groups]

@api_router.get("/groups/{group_id}")
async def get_group(group_id: str, request: Request):
    current_user = await get_current_user(request)
    
    # Verificar acceso al grupo
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if current_user.get("group_id") != group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
    
    group = await db.groups.find_one({"_id": ObjectId(group_id)})
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    return serialize_doc(group)

@api_router.put("/groups/{group_id}")
async def update_group(group_id: str, group_data: GroupCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.groups.update_one({"_id": ObjectId(group_id)}, {"$set": {"name": group_data.name, "description": group_data.description}})
    group = await db.groups.find_one({"_id": ObjectId(group_id)})
    return serialize_doc(group)

# ============== BRANDS ROUTES ==============

@api_router.post("/brands")
async def create_brand(brand_data: BrandCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    brand_doc = {
        "name": brand_data.name,
        "group_id": brand_data.group_id,
        "logo_url": brand_data.logo_url,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.brands.insert_one(brand_doc)
    brand_doc["id"] = str(result.inserted_id)
    return serialize_doc(brand_doc)

@api_router.get("/brands")
async def get_brands(request: Request, group_id: Optional[str] = None):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Super admin y super users pueden ver todas las marcas o filtrar por grupo
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if group_id:
            query["group_id"] = group_id
    else:
        # Otros roles solo ven marcas de su grupo
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        
        # Si se pasa un group_id, verificar que sea el suyo
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a las marcas de este grupo")
        
        query["group_id"] = user_group_id
    
    brands = await db.brands.find(query).to_list(1000)
    return [serialize_doc(b) for b in brands]

@api_router.put("/brands/{brand_id}")
async def update_brand(brand_id: str, brand_data: BrandCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.brands.update_one({"_id": ObjectId(brand_id)}, {"$set": {"name": brand_data.name, "logo_url": brand_data.logo_url}})
    brand = await db.brands.find_one({"_id": ObjectId(brand_id)})
    return serialize_doc(brand)

# ============== AGENCIES ROUTES ==============

@api_router.post("/agencies")
async def create_agency(agency_data: AgencyCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get brand to link group_id
    brand = await db.brands.find_one({"_id": ObjectId(agency_data.brand_id)})
    if not brand:
        raise HTTPException(status_code=404, detail="Brand not found")
    
    agency_doc = {
        "name": agency_data.name,
        "brand_id": agency_data.brand_id,
        "group_id": brand["group_id"],
        "address": agency_data.address,
        "city": agency_data.city,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.agencies.insert_one(agency_doc)
    agency_doc["id"] = str(result.inserted_id)
    return serialize_doc(agency_doc)

@api_router.get("/agencies")
async def get_agencies(request: Request, brand_id: Optional[str] = None, group_id: Optional[str] = None):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Super admin y super users pueden ver todas las agencias o filtrar
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if brand_id:
            query["brand_id"] = brand_id
        if group_id:
            query["group_id"] = group_id
    else:
        # Otros roles solo ven agencias de su grupo
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        
        # Si se pasa un group_id, verificar que sea el suyo
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a las agencias de este grupo")
        
        query["group_id"] = user_group_id
        
        if brand_id:
            query["brand_id"] = brand_id
    
    agencies = await db.agencies.find(query).to_list(1000)
    
    # Enrich with brand names
    brand_ids = list(set(a.get("brand_id") for a in agencies if a.get("brand_id")))
    brands = await db.brands.find({"_id": {"$in": [ObjectId(b) for b in brand_ids]}}).to_list(1000)
    brand_map = {str(b["_id"]): b["name"] for b in brands}
    
    result = []
    for a in agencies:
        agency = serialize_doc(a)
        agency["brand_name"] = brand_map.get(a.get("brand_id"), "")
        result.append(agency)
    
    return result

@api_router.put("/agencies/{agency_id}")
async def update_agency(agency_id: str, agency_data: AgencyCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.agencies.update_one({"_id": ObjectId(agency_id)}, {"$set": {
        "name": agency_data.name,
        "address": agency_data.address,
        "city": agency_data.city
    }})
    agency = await db.agencies.find_one({"_id": ObjectId(agency_id)})
    return serialize_doc(agency)

# ============== FINANCIAL RATES ROUTES ==============

@api_router.post("/financial-rates")
async def create_financial_rate(rate_data: FinancialRateCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    rate_doc = {
        "group_id": rate_data.group_id,
        "brand_id": rate_data.brand_id,
        "agency_id": rate_data.agency_id,
        "tiie_rate": rate_data.tiie_rate,
        "spread": rate_data.spread,
        "grace_days": rate_data.grace_days,
        "name": rate_data.name,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.financial_rates.insert_one(rate_doc)
    rate_doc["id"] = str(result.inserted_id)
    rate_doc["total_rate"] = rate_data.tiie_rate + rate_data.spread
    return serialize_doc(rate_doc)

@api_router.get("/financial-rates")
async def get_financial_rates(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None
):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Control de acceso por roles
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id
    else:
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
        query["group_id"] = user_group_id
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
    
    rates = await db.financial_rates.find(query).to_list(1000)
    
    # Enrich with total_rate and names
    result = []
    for r in rates:
        rate = serialize_doc(r)
        tiie = r.get("tiie_rate", r.get("annual_rate", 11.25))  # Backwards compatible
        spread = r.get("spread", 0)
        rate["tiie_rate"] = tiie
        rate["spread"] = spread
        rate["total_rate"] = tiie + spread
        
        # Get brand/agency names
        if r.get("brand_id"):
            brand = await db.brands.find_one({"_id": ObjectId(r["brand_id"])})
            if brand:
                rate["brand_name"] = brand["name"]
        if r.get("agency_id"):
            agency = await db.agencies.find_one({"_id": ObjectId(r["agency_id"])})
            if agency:
                rate["agency_name"] = agency["name"]
        if r.get("group_id"):
            group = await db.groups.find_one({"_id": ObjectId(r["group_id"])})
            if group:
                rate["group_name"] = group["name"]
        
        result.append(rate)
    
    return result

@api_router.put("/financial-rates/{rate_id}")
async def update_financial_rate(rate_id: str, rate_data: FinancialRateCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.financial_rates.update_one({"_id": ObjectId(rate_id)}, {"$set": {
        "tiie_rate": rate_data.tiie_rate,
        "spread": rate_data.spread,
        "grace_days": rate_data.grace_days,
        "name": rate_data.name
    }})
    rate = await db.financial_rates.find_one({"_id": ObjectId(rate_id)})
    result = serialize_doc(rate)
    result["total_rate"] = rate.get("tiie_rate", 11.25) + rate.get("spread", 0)
    return result

@api_router.delete("/financial-rates/{rate_id}")
async def delete_financial_rate(rate_id: str, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.financial_rates.delete_one({"_id": ObjectId(rate_id)})
    return {"message": "Rate deleted"}

# ============== VEHICLES ROUTES ==============

async def calculate_vehicle_financial_cost(vehicle: dict) -> float:
    """Calculate the financial cost based on aging and rate (TIIE + spread)"""
    entry_date = vehicle.get("entry_date")
    if isinstance(entry_date, str):
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
    elif isinstance(entry_date, datetime) and entry_date.tzinfo is None:
        entry_date = entry_date.replace(tzinfo=timezone.utc)
    
    if vehicle.get("exit_date"):
        end_date = vehicle["exit_date"]
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        elif isinstance(end_date, datetime) and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)
    
    aging_days = (end_date - entry_date).days
    
    # Find applicable rate (agency > brand > group priority)
    rate = await db.financial_rates.find_one({
        "$or": [
            {"agency_id": vehicle.get("agency_id")},
            {"brand_id": vehicle.get("brand_id")},
            {"group_id": vehicle.get("group_id")}
        ]
    }, sort=[("agency_id", -1), ("brand_id", -1)])
    
    if not rate:
        # Default: TIIE 11.25% + 2% spread = 13.25%
        rate = {"tiie_rate": 11.25, "spread": 2.0, "grace_days": 0}
    
    # Calculate total rate (TIIE + spread)
    tiie = rate.get("tiie_rate", rate.get("annual_rate", 11.25))
    spread = rate.get("spread", 0)
    total_rate = tiie + spread
    
    effective_days = max(0, aging_days - rate.get("grace_days", 0))
    daily_rate = total_rate / 365 / 100
    financial_cost = vehicle["purchase_price"] * daily_rate * effective_days
    
    return round(financial_cost, 2)

async def enrich_vehicle(vehicle: dict) -> dict:
    """Enrich vehicle with agency, brand, group info and calculations"""
    result = serialize_doc(vehicle)
    
    # Get agency info
    if vehicle.get("agency_id"):
        agency = await db.agencies.find_one({"_id": ObjectId(vehicle["agency_id"])})
        if agency:
            result["agency_name"] = agency["name"]
            result["brand_id"] = agency.get("brand_id")
            result["group_id"] = agency.get("group_id")
            
            # Get brand name
            if agency.get("brand_id"):
                brand = await db.brands.find_one({"_id": ObjectId(agency["brand_id"])})
                if brand:
                    result["brand_name"] = brand["name"]
    
    # Calculate aging
    entry_date = vehicle.get("entry_date")
    if isinstance(entry_date, str):
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
    elif isinstance(entry_date, datetime) and entry_date.tzinfo is None:
        entry_date = entry_date.replace(tzinfo=timezone.utc)
    
    if vehicle.get("exit_date"):
        end_date = vehicle["exit_date"]
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        elif isinstance(end_date, datetime) and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)
    
    result["aging_days"] = (end_date - entry_date).days
    result["financial_cost"] = await calculate_vehicle_financial_cost(vehicle)
    
    return result

@api_router.post("/vehicles")
async def create_vehicle(vehicle_data: VehicleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get agency to link brand and group
    agency = await db.agencies.find_one({"_id": ObjectId(vehicle_data.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    
    entry_date = vehicle_data.entry_date
    if entry_date:
        entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
    else:
        entry_date = datetime.now(timezone.utc)
    
    vehicle_doc = {
        "vin": vehicle_data.vin,
        "model": vehicle_data.model,
        "year": vehicle_data.year,
        "trim": vehicle_data.trim,
        "color": vehicle_data.color,
        "vehicle_type": vehicle_data.vehicle_type,
        "purchase_price": vehicle_data.purchase_price,
        "agency_id": vehicle_data.agency_id,
        "brand_id": agency.get("brand_id"),
        "group_id": agency.get("group_id"),
        "entry_date": entry_date,
        "exit_date": None,
        "status": "in_stock",
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.vehicles.insert_one(vehicle_doc)
    vehicle_doc["_id"] = result.inserted_id
    return await enrich_vehicle(vehicle_doc)

@api_router.get("/vehicles")
async def get_vehicles(
    request: Request,
    agency_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    group_id: Optional[str] = None,
    status: Optional[str] = None,
    vehicle_type: Optional[str] = None
):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Super admin y super users pueden ver todos los vehículos o filtrar
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if agency_id:
            query["agency_id"] = agency_id
        if brand_id:
            query["brand_id"] = brand_id
        if group_id:
            query["group_id"] = group_id
    else:
        # Otros roles solo ven vehículos de su grupo
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        
        # Si se pasa un group_id, verificar que sea el suyo
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a los vehículos de este grupo")
        
        query["group_id"] = user_group_id
        
        if agency_id:
            query["agency_id"] = agency_id
        if brand_id:
            query["brand_id"] = brand_id
    
    if status:
        query["status"] = status
    if vehicle_type:
        query["vehicle_type"] = vehicle_type
    
    vehicles = await db.vehicles.find(query).to_list(1000)
    return [await enrich_vehicle(v) for v in vehicles]

@api_router.get("/vehicles/{vehicle_id}")
async def get_vehicle(vehicle_id: str, request: Request):
    await get_current_user(request)
    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    return await enrich_vehicle(vehicle)

@api_router.put("/vehicles/{vehicle_id}")
async def update_vehicle(vehicle_id: str, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    data = await request.json()
    update_data = {k: v for k, v in data.items() if k not in ["id", "_id"]}
    
    await db.vehicles.update_one({"_id": ObjectId(vehicle_id)}, {"$set": update_data})
    vehicle = await db.vehicles.find_one({"_id": ObjectId(vehicle_id)})
    return await enrich_vehicle(vehicle)

# ============== SALES OBJECTIVES ROUTES ==============

@api_router.post("/sales-objectives")
async def create_sales_objective(objective_data: SalesObjectiveCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get agency to link brand and group
    agency = await db.agencies.find_one({"_id": ObjectId(objective_data.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    
    objective_doc = {
        "seller_id": objective_data.seller_id,
        "agency_id": objective_data.agency_id,
        "brand_id": agency.get("brand_id"),
        "group_id": agency.get("group_id"),
        "month": objective_data.month,
        "year": objective_data.year,
        "units_target": objective_data.units_target,
        "revenue_target": objective_data.revenue_target,
        "vehicle_line": objective_data.vehicle_line,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.sales_objectives.insert_one(objective_doc)
    objective_doc["id"] = str(result.inserted_id)
    return serialize_doc(objective_doc)

@api_router.get("/sales-objectives")
async def get_sales_objectives(
    request: Request,
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None
):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Control de acceso por roles
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if seller_id:
            query["seller_id"] = seller_id
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id
    else:
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
        query["group_id"] = user_group_id
        if seller_id:
            query["seller_id"] = seller_id
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
    
    if month:
        query["month"] = month
    if year:
        query["year"] = year
    
    objectives = await db.sales_objectives.find(query).to_list(1000)
    
    # Enrich with progress data
    result = []
    for obj in objectives:
        serialized = serialize_doc(obj)
        
        # Get seller name
        if obj.get("seller_id"):
            seller = await db.users.find_one({"_id": ObjectId(obj["seller_id"])})
            if seller:
                serialized["seller_name"] = seller["name"]
        
        # Get agency name
        if obj.get("agency_id"):
            agency = await db.agencies.find_one({"_id": ObjectId(obj["agency_id"])})
            if agency:
                serialized["agency_name"] = agency["name"]
        
        # Get brand name
        if obj.get("brand_id"):
            brand = await db.brands.find_one({"_id": ObjectId(obj["brand_id"])})
            if brand:
                serialized["brand_name"] = brand["name"]
        
        # Get group name
        if obj.get("group_id"):
            group = await db.groups.find_one({"_id": ObjectId(obj["group_id"])})
            if group:
                serialized["group_name"] = group["name"]
        
        # Calculate progress
        start_date = datetime(obj["year"], obj["month"], 1, tzinfo=timezone.utc)
        if obj["month"] == 12:
            end_date = datetime(obj["year"] + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(obj["year"], obj["month"] + 1, 1, tzinfo=timezone.utc)
        
        sales_query = {"sale_date": {"$gte": start_date, "$lt": end_date}}
        if obj.get("seller_id"):
            sales_query["seller_id"] = obj["seller_id"]
        elif obj.get("agency_id"):
            sales_query["agency_id"] = obj["agency_id"]
        
        sales = await db.sales.find(sales_query).to_list(1000)
        serialized["units_sold"] = len(sales)
        serialized["revenue_achieved"] = sum(s.get("sale_price", 0) for s in sales)
        serialized["commissions_achieved"] = sum(s.get("commission", 0) for s in sales)
        serialized["progress_units"] = round((serialized["units_sold"] / obj["units_target"] * 100) if obj["units_target"] > 0 else 0, 1)
        serialized["progress_revenue"] = round((serialized["revenue_achieved"] / obj["revenue_target"] * 100) if obj["revenue_target"] > 0 else 0, 1)
        
        result.append(serialized)
    
    return result

@api_router.put("/sales-objectives/{objective_id}")
async def update_sales_objective(objective_id: str, objective_data: SalesObjectiveCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.sales_objectives.update_one({"_id": ObjectId(objective_id)}, {"$set": {
        "units_target": objective_data.units_target,
        "revenue_target": objective_data.revenue_target,
        "vehicle_line": objective_data.vehicle_line
    }})
    objective = await db.sales_objectives.find_one({"_id": ObjectId(objective_id)})
    return serialize_doc(objective)

# ============== COMMISSION RULES ROUTES ==============

@api_router.post("/commission-rules")
async def create_commission_rule(rule_data: CommissionRuleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get agency to link brand and group
    agency = await db.agencies.find_one({"_id": ObjectId(rule_data.agency_id)})
    if not agency:
        raise HTTPException(status_code=404, detail="Agency not found")
    
    rule_doc = {
        "agency_id": rule_data.agency_id,
        "brand_id": agency.get("brand_id"),
        "group_id": agency.get("group_id"),
        "name": rule_data.name,
        "rule_type": rule_data.rule_type,
        "value": rule_data.value,
        "min_units": rule_data.min_units,
        "max_units": rule_data.max_units,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.commission_rules.insert_one(rule_doc)
    rule_doc["id"] = str(result.inserted_id)
    return serialize_doc(rule_doc)

@api_router.get("/commission-rules")
async def get_commission_rules(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None
):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Control de acceso por roles
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id
    else:
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a este grupo")
        query["group_id"] = user_group_id
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
    
    rules = await db.commission_rules.find(query).to_list(1000)
    
    # Enrich with names
    result = []
    for r in rules:
        rule = serialize_doc(r)
        if r.get("agency_id"):
            agency = await db.agencies.find_one({"_id": ObjectId(r["agency_id"])})
            if agency:
                rule["agency_name"] = agency["name"]
        if r.get("brand_id"):
            brand = await db.brands.find_one({"_id": ObjectId(r["brand_id"])})
            if brand:
                rule["brand_name"] = brand["name"]
        if r.get("group_id"):
            group = await db.groups.find_one({"_id": ObjectId(r["group_id"])})
            if group:
                rule["group_name"] = group["name"]
        result.append(rule)
    
    return result

@api_router.put("/commission-rules/{rule_id}")
async def update_commission_rule(rule_id: str, rule_data: CommissionRuleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.commission_rules.update_one({"_id": ObjectId(rule_id)}, {"$set": {
        "name": rule_data.name,
        "rule_type": rule_data.rule_type,
        "value": rule_data.value,
        "min_units": rule_data.min_units,
        "max_units": rule_data.max_units
    }})
    rule = await db.commission_rules.find_one({"_id": ObjectId(rule_id)})
    return serialize_doc(rule)

@api_router.delete("/commission-rules/{rule_id}")
async def delete_commission_rule(rule_id: str, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.commission_rules.delete_one({"_id": ObjectId(rule_id)})
    return {"message": "Rule deleted"}

# ============== SALES ROUTES ==============

async def calculate_commission(sale: dict, agency_id: str, seller_id: str) -> float:
    """Calculate commission based on rules"""
    rules = await db.commission_rules.find({"agency_id": agency_id}).to_list(100)
    
    # Get seller's monthly sales count
    now = datetime.now(timezone.utc)
    start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    seller_sales = await db.sales.count_documents({
        "seller_id": seller_id,
        "agency_id": agency_id,
        "sale_date": {"$gte": start_of_month}
    })
    
    total_commission = 0
    
    for rule in rules:
        if rule["rule_type"] == "per_unit":
            total_commission += rule["value"]
        elif rule["rule_type"] == "percentage":
            total_commission += sale["sale_price"] * (rule["value"] / 100)
        elif rule["rule_type"] == "volume_bonus":
            if rule.get("min_units") and seller_sales >= rule["min_units"]:
                if not rule.get("max_units") or seller_sales <= rule["max_units"]:
                    total_commission += rule["value"]
        elif rule["rule_type"] == "fi_bonus":
            total_commission += sale.get("fi_revenue", 0) * (rule["value"] / 100)
    
    return round(total_commission, 2)

@api_router.post("/sales")
async def create_sale(sale_data: SaleCreate, request: Request):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN, UserRole.SELLER]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get vehicle and mark as sold
    vehicle = await db.vehicles.find_one({"_id": ObjectId(sale_data.vehicle_id)})
    if not vehicle:
        raise HTTPException(status_code=404, detail="Vehicle not found")
    
    sale_date = sale_data.sale_date
    if sale_date:
        sale_date = datetime.fromisoformat(sale_date.replace("Z", "+00:00"))
    else:
        sale_date = datetime.now(timezone.utc)
    
    # Calculate commission
    commission = await calculate_commission(
        {"sale_price": sale_data.sale_price, "fi_revenue": sale_data.fi_revenue},
        vehicle["agency_id"],
        sale_data.seller_id
    )
    
    sale_doc = {
        "vehicle_id": sale_data.vehicle_id,
        "seller_id": sale_data.seller_id,
        "agency_id": vehicle["agency_id"],
        "brand_id": vehicle.get("brand_id"),
        "group_id": vehicle.get("group_id"),
        "sale_price": sale_data.sale_price,
        "sale_date": sale_date,
        "fi_revenue": sale_data.fi_revenue,
        "commission": commission,
        "created_at": datetime.now(timezone.utc)
    }
    result = await db.sales.insert_one(sale_doc)
    
    # Mark vehicle as sold
    await db.vehicles.update_one(
        {"_id": ObjectId(sale_data.vehicle_id)},
        {"$set": {"status": "sold", "exit_date": sale_date}}
    )
    
    sale_doc["id"] = str(result.inserted_id)
    return serialize_doc(sale_doc)

@api_router.get("/sales")
async def get_sales(
    request: Request,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None,
    month: Optional[int] = None,
    year: Optional[int] = None
):
    current_user = await get_current_user(request)
    
    query = {}
    if agency_id:
        query["agency_id"] = agency_id
    if seller_id:
        query["seller_id"] = seller_id
    if month and year:
        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        query["sale_date"] = {"$gte": start_date, "$lt": end_date}
    
    sales = await db.sales.find(query).to_list(1000)
    
    # Enrich with vehicle and seller info
    result = []
    for sale in sales:
        serialized = serialize_doc(sale)
        
        # Get vehicle info
        if sale.get("vehicle_id"):
            vehicle = await db.vehicles.find_one({"_id": ObjectId(sale["vehicle_id"])})
            if vehicle:
                serialized["vehicle_info"] = {
                    "model": vehicle["model"],
                    "year": vehicle["year"],
                    "trim": vehicle["trim"],
                    "color": vehicle["color"],
                    "vin": vehicle["vin"]
                }
        
        # Get seller name
        if sale.get("seller_id"):
            seller = await db.users.find_one({"_id": ObjectId(sale["seller_id"])})
            if seller:
                serialized["seller_name"] = seller["name"]
        
        result.append(serialized)
    
    return result

# ============== DASHBOARD / ANALYTICS ROUTES ==============

@api_router.get("/dashboard/kpis")
async def get_dashboard_kpis(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None
):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Super admin y super users pueden ver todos los datos o filtrar
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id
        # Si no hay filtro, ve todo
    else:
        # Otros roles solo ven datos de su grupo
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return {
                "total_vehicles": 0, "total_value": 0, "total_financial_cost": 0,
                "avg_aging_days": 0, "aging_buckets": {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0},
                "units_sold_month": 0, "revenue_month": 0, "commissions_month": 0,
                "new_vehicles": 0, "used_vehicles": 0
            }
        
        # Si se pasa un group_id, verificar que sea el suyo
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a los datos de este grupo")
        
        query["group_id"] = user_group_id
        
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
    
    # Get vehicles in stock
    in_stock_query = {**query, "status": "in_stock"}
    vehicles_in_stock = await db.vehicles.find(in_stock_query).to_list(10000)
    
    total_vehicles = len(vehicles_in_stock)
    total_value = sum(v.get("purchase_price", 0) for v in vehicles_in_stock)
    
    # Calculate total financial cost and average aging
    total_financial_cost = 0
    total_aging = 0
    aging_buckets = {"0-30": 0, "31-60": 0, "61-90": 0, "90+": 0}
    
    for v in vehicles_in_stock:
        enriched = await enrich_vehicle(v)
        total_financial_cost += enriched.get("financial_cost", 0)
        aging = enriched.get("aging_days", 0)
        total_aging += aging
        
        if aging <= 30:
            aging_buckets["0-30"] += 1
        elif aging <= 60:
            aging_buckets["31-60"] += 1
        elif aging <= 90:
            aging_buckets["61-90"] += 1
        else:
            aging_buckets["90+"] += 1
    
    avg_aging = round(total_aging / total_vehicles, 1) if total_vehicles > 0 else 0
    
    # Get current month sales
    now = datetime.now(timezone.utc)
    start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    sales_query = {**query, "sale_date": {"$gte": start_of_month}}
    if seller_id:
        sales_query["seller_id"] = seller_id
    monthly_sales = await db.sales.find(sales_query).to_list(10000)
    
    units_sold_month = len(monthly_sales)
    revenue_month = sum(s.get("sale_price", 0) for s in monthly_sales)
    commissions_month = sum(s.get("commission", 0) for s in monthly_sales)
    
    return {
        "total_vehicles": total_vehicles,
        "total_value": round(total_value, 2),
        "total_financial_cost": round(total_financial_cost, 2),
        "avg_aging_days": avg_aging,
        "aging_buckets": aging_buckets,
        "units_sold_month": units_sold_month,
        "revenue_month": round(revenue_month, 2),
        "commissions_month": round(commissions_month, 2),
        "new_vehicles": len([v for v in vehicles_in_stock if v.get("vehicle_type") == "new"]),
        "used_vehicles": len([v for v in vehicles_in_stock if v.get("vehicle_type") == "used"])
    }

@api_router.get("/dashboard/trends")
async def get_sales_trends(
    request: Request, 
    group_id: Optional[str] = None,
    brand_id: Optional[str] = None,
    agency_id: Optional[str] = None,
    seller_id: Optional[str] = None,
    months: int = 6
):
    current_user = await get_current_user(request)
    
    query = {}
    
    # Super admin y super users pueden ver todos los datos o filtrar
    if current_user["role"] in [UserRole.APP_ADMIN, UserRole.APP_USER]:
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
        elif group_id:
            query["group_id"] = group_id
    else:
        # Otros roles solo ven datos de su grupo
        user_group_id = current_user.get("group_id")
        if not user_group_id:
            return []
        
        if group_id and group_id != user_group_id:
            raise HTTPException(status_code=403, detail="No tienes acceso a los datos de este grupo")
        
        query["group_id"] = user_group_id
        
        if agency_id:
            query["agency_id"] = agency_id
        elif brand_id:
            query["brand_id"] = brand_id
    
    if seller_id:
        query["seller_id"] = seller_id
    
    now = datetime.now(timezone.utc)
    trends = []
    
    for i in range(months - 1, -1, -1):
        month_date = now - timedelta(days=30 * i)
        year = month_date.year
        month = month_date.month
        
        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        
        sales_query = {**query, "sale_date": {"$gte": start_date, "$lt": end_date}}
        sales = await db.sales.find(sales_query).to_list(10000)
        
        trends.append({
            "month": f"{year}-{month:02d}",
            "units": len(sales),
            "revenue": round(sum(s.get("sale_price", 0) for s in sales), 2),
            "commission": round(sum(s.get("commission", 0) for s in sales), 2)
        })
    
    return trends

@api_router.get("/dashboard/seller-performance")
async def get_seller_performance(request: Request, agency_id: Optional[str] = None, month: Optional[int] = None, year: Optional[int] = None):
    current_user = await get_current_user(request)
    
    now = datetime.now(timezone.utc)
    if not month:
        month = now.month
    if not year:
        year = now.year
    
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    
    query = {"sale_date": {"$gte": start_date, "$lt": end_date}}
    if agency_id:
        query["agency_id"] = agency_id
    
    sales = await db.sales.find(query).to_list(10000)
    
    # Group by seller
    seller_stats = {}
    for sale in sales:
        seller_id = sale.get("seller_id")
        if seller_id not in seller_stats:
            seller_stats[seller_id] = {
                "units": 0,
                "revenue": 0,
                "commission": 0
            }
        seller_stats[seller_id]["units"] += 1
        seller_stats[seller_id]["revenue"] += sale.get("sale_price", 0)
        seller_stats[seller_id]["commission"] += sale.get("commission", 0)
    
    # Get seller names
    result = []
    for seller_id, stats in seller_stats.items():
        seller = await db.users.find_one({"_id": ObjectId(seller_id)})
        result.append({
            "seller_id": seller_id,
            "seller_name": seller["name"] if seller else "Unknown",
            "units": stats["units"],
            "revenue": round(stats["revenue"], 2),
            "commission": round(stats["commission"], 2)
        })
    
    return sorted(result, key=lambda x: x["units"], reverse=True)

@api_router.get("/dashboard/suggestions")
async def get_vehicle_suggestions(request: Request, group_id: Optional[str] = None):
    """Get smart suggestions for vehicles that should be promoted/discounted"""
    current_user = await get_current_user(request)
    
    query = {"status": "in_stock"}
    if group_id:
        query["group_id"] = group_id
    elif current_user.get("group_id"):
        query["group_id"] = current_user["group_id"]
    
    vehicles = await db.vehicles.find(query).to_list(1000)
    suggestions = []
    
    for vehicle in vehicles:
        enriched = await enrich_vehicle(vehicle)
        aging = enriched.get("aging_days", 0)
        
        # Find average days to sell for similar vehicles
        similar_query = {
            "model": vehicle["model"],
            "trim": vehicle["trim"],
            "color": vehicle["color"],
            "status": "sold",
            "group_id": vehicle.get("group_id")
        }
        similar_sold = await db.vehicles.find(similar_query).to_list(100)
        
        if similar_sold:
            avg_days = sum(
                (v.get("exit_date", datetime.now(timezone.utc)) - v.get("entry_date", datetime.now(timezone.utc))).days 
                if isinstance(v.get("exit_date"), datetime) and isinstance(v.get("entry_date"), datetime)
                else 60
                for v in similar_sold
            ) / len(similar_sold)
        else:
            avg_days = 60  # Default assumption
        
        # Suggest if aging is more than half the average
        if aging > avg_days / 2:
            financial_cost = enriched.get("financial_cost", 0)
            projected_additional_cost = (avg_days - aging) * (vehicle["purchase_price"] * 0.12 / 365)
            suggested_bonus = min(projected_additional_cost * 0.5, vehicle["purchase_price"] * 0.02)
            
            suggestions.append({
                "vehicle_id": enriched["id"],
                "vehicle_info": {
                    "model": vehicle["model"],
                    "year": vehicle["year"],
                    "trim": vehicle["trim"],
                    "color": vehicle["color"],
                    "vin": vehicle["vin"],
                    "purchase_price": vehicle["purchase_price"]
                },
                "avg_days_to_sell": round(avg_days),
                "current_aging": aging,
                "financial_cost": financial_cost,
                "suggested_bonus": round(suggested_bonus, 2),
                "reason": f"Este vehículo lleva {aging} días en inventario. Vehículos similares se venden en promedio en {round(avg_days)} días."
            })
    
    return sorted(suggestions, key=lambda x: x["current_aging"], reverse=True)[:20]

# ============== IMPORT ROUTES ==============

@api_router.post("/import/vehicles")
async def import_vehicles(request: Request, file: UploadFile = File(...)):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    content = await file.read()
    
    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(BytesIO(content))
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(BytesIO(content))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Use CSV or Excel.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")
    
    required_columns = ['vin', 'model', 'year', 'trim', 'color', 'vehicle_type', 'purchase_price', 'agency_id']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")
    
    imported = 0
    errors = []
    
    for idx, row in df.iterrows():
        try:
            agency = await db.agencies.find_one({"_id": ObjectId(row['agency_id'])})
            if not agency:
                errors.append(f"Row {idx + 2}: Agency not found")
                continue
            
            entry_date = row.get('entry_date')
            if pd.isna(entry_date):
                entry_date = datetime.now(timezone.utc)
            elif isinstance(entry_date, str):
                entry_date = datetime.fromisoformat(entry_date.replace("Z", "+00:00"))
            
            vehicle_doc = {
                "vin": str(row['vin']),
                "model": str(row['model']),
                "year": int(row['year']),
                "trim": str(row['trim']),
                "color": str(row['color']),
                "vehicle_type": str(row['vehicle_type']),
                "purchase_price": float(row['purchase_price']),
                "agency_id": str(row['agency_id']),
                "brand_id": agency.get("brand_id"),
                "group_id": agency.get("group_id"),
                "entry_date": entry_date,
                "exit_date": None,
                "status": "in_stock",
                "created_at": datetime.now(timezone.utc)
            }
            
            await db.vehicles.insert_one(vehicle_doc)
            imported += 1
        except Exception as e:
            errors.append(f"Row {idx + 2}: {str(e)}")
    
    return {
        "imported": imported,
        "errors": errors,
        "total_rows": len(df)
    }

@api_router.post("/import/sales")
async def import_sales(request: Request, file: UploadFile = File(...)):
    current_user = await get_current_user(request)
    if current_user["role"] not in [UserRole.APP_ADMIN, UserRole.GROUP_ADMIN, UserRole.BRAND_ADMIN, UserRole.AGENCY_ADMIN]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    content = await file.read()
    
    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(BytesIO(content))
        elif file.filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(BytesIO(content))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Use CSV or Excel.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")
    
    required_columns = ['vehicle_id', 'seller_id', 'sale_price', 'sale_date']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required columns: {missing}")
    
    imported = 0
    errors = []
    
    for idx, row in df.iterrows():
        try:
            vehicle = await db.vehicles.find_one({"_id": ObjectId(row['vehicle_id'])})
            if not vehicle:
                errors.append(f"Row {idx + 2}: Vehicle not found")
                continue
            
            sale_date = row['sale_date']
            if isinstance(sale_date, str):
                sale_date = datetime.fromisoformat(sale_date.replace("Z", "+00:00"))
            
            fi_revenue = float(row.get('fi_revenue', 0)) if not pd.isna(row.get('fi_revenue')) else 0
            
            commission = await calculate_commission(
                {"sale_price": float(row['sale_price']), "fi_revenue": fi_revenue},
                vehicle["agency_id"],
                str(row['seller_id'])
            )
            
            sale_doc = {
                "vehicle_id": str(row['vehicle_id']),
                "seller_id": str(row['seller_id']),
                "agency_id": vehicle["agency_id"],
                "brand_id": vehicle.get("brand_id"),
                "group_id": vehicle.get("group_id"),
                "sale_price": float(row['sale_price']),
                "sale_date": sale_date,
                "fi_revenue": fi_revenue,
                "commission": commission,
                "created_at": datetime.now(timezone.utc)
            }
            
            await db.sales.insert_one(sale_doc)
            
            # Mark vehicle as sold
            await db.vehicles.update_one(
                {"_id": ObjectId(row['vehicle_id'])},
                {"$set": {"status": "sold", "exit_date": sale_date}}
            )
            
            imported += 1
        except Exception as e:
            errors.append(f"Row {idx + 2}: {str(e)}")
    
    return {
        "imported": imported,
        "errors": errors,
        "total_rows": len(df)
    }

# ============== ROOT ROUTE ==============

@api_router.get("/")
async def root():
    return {"message": "AutoConnect API - Vehicle Inventory Management System"}

@api_router.get("/health")
async def health():
    return {"status": "healthy"}

# Include the router in the main app
app.include_router(api_router)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=[os.environ.get('FRONTEND_URL', 'http://localhost:3000'), "*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============== STARTUP ==============

async def seed_admin():
    """Seed admin user on startup"""
    admin_email = os.environ.get("ADMIN_EMAIL", "admin@autoconnect.com")
    admin_password = os.environ.get("ADMIN_PASSWORD", "Admin123!")
    
    existing = await db.users.find_one({"email": admin_email})
    if existing is None:
        hashed = hash_password(admin_password)
        await db.users.insert_one({
            "email": admin_email,
            "password_hash": hashed,
            "name": "Admin",
            "role": UserRole.APP_ADMIN,
            "group_id": None,
            "brand_id": None,
            "agency_id": None,
            "created_at": datetime.now(timezone.utc)
        })
        logger.info(f"Admin user created: {admin_email}")
    elif not verify_password(admin_password, existing["password_hash"]):
        await db.users.update_one(
            {"email": admin_email},
            {"$set": {"password_hash": hash_password(admin_password)}}
        )
        logger.info(f"Admin password updated: {admin_email}")

async def create_indexes():
    """Create MongoDB indexes"""
    await db.users.create_index("email", unique=True)
    await db.vehicles.create_index("vin")
    await db.vehicles.create_index("agency_id")
    await db.vehicles.create_index("status")
    await db.sales.create_index("seller_id")
    await db.sales.create_index("agency_id")
    await db.sales.create_index("sale_date")

@app.on_event("startup")
async def startup():
    await create_indexes()
    await seed_admin()
    logger.info("AutoConnect API started")

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
