import { useState, useEffect, useCallback, useMemo } from 'react';
import { groupsApi, brandsApi, agenciesApi, usersApi, auditLogsApi, organizationImportApi, authApi, sellersApi, vehicleCatalogApi } from '../lib/api';
import { useAuth } from '../contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Checkbox } from '../components/ui/checkbox';
import { Skeleton } from '../components/ui/skeleton';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger
} from '../components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '../components/ui/table';
import {
  Plus,
  Buildings,
  Factory,
  Users,
  UserPlus,
  UploadSimple,
  Pencil,
  Shield,
  Trash
} from '@phosphor-icons/react';
import { toast } from 'sonner';

const ROLE_LABELS = {
  app_admin: 'Admin App',
  app_user: 'Usuario App',
  group_admin: 'Admin Grupo',
  group_finance_manager: 'Finanzas Grupo',
  brand_admin: 'Admin Marca',
  agency_admin: 'Gerente General (Legacy)',
  agency_sales_manager: 'Gerente de Ventas',
  agency_general_manager: 'Gerente General',
  agency_commercial_manager: 'Gerente Comercial (Legacy)',
  group_user: 'Usuario Grupo',
  brand_user: 'Usuario Marca',
  agency_user: 'Lectura (Legacy)',
  seller: 'Vendedor'
};

const ROLES = [
  { value: 'app_admin', label: ROLE_LABELS.app_admin },
  { value: 'app_user', label: ROLE_LABELS.app_user },
  { value: 'group_admin', label: ROLE_LABELS.group_admin },
  { value: 'group_finance_manager', label: ROLE_LABELS.group_finance_manager },
  { value: 'brand_admin', label: ROLE_LABELS.brand_admin },
  { value: 'agency_admin', label: ROLE_LABELS.agency_admin },
  { value: 'agency_sales_manager', label: ROLE_LABELS.agency_sales_manager },
  { value: 'agency_general_manager', label: ROLE_LABELS.agency_general_manager },
  { value: 'agency_commercial_manager', label: ROLE_LABELS.agency_commercial_manager },
  { value: 'group_user', label: ROLE_LABELS.group_user },
  { value: 'brand_user', label: ROLE_LABELS.brand_user },
  { value: 'agency_user', label: ROLE_LABELS.agency_user },
  { value: 'seller', label: ROLE_LABELS.seller }
];

const getRoleLabel = (roleValue) => ROLE_LABELS[roleValue] || roleValue || 'Sin rol';
const getAgencyAccessLabel = (roleValue) => {
  if (roleValue === 'agency_sales_manager') return 'Escritura';
  if (roleValue === 'agency_user') return 'Lectura (Legacy)';
  return getRoleLabel(roleValue);
};

const APP_LEVEL_ROLES = ['app_admin', 'app_user'];
const BRAND_SCOPED_ROLES = ['brand_admin', 'brand_user'];
const AGENCY_SCOPED_ROLES = [
  'agency_admin',
  'agency_sales_manager',
  'agency_general_manager',
  'agency_commercial_manager',
  'agency_user',
  'seller'
];
const DEALER_GENERAL_EFFECTIVE_ROLES = ['agency_general_manager', 'agency_admin', 'agency_commercial_manager'];
const DEALER_SALES_EFFECTIVE_ROLES = ['agency_sales_manager'];
const DEALER_MANAGER_ROLES = [...DEALER_GENERAL_EFFECTIVE_ROLES, ...DEALER_SALES_EFFECTIVE_ROLES];
const DEALER_GENERAL_MANAGEABLE_ROLES = ['agency_sales_manager', 'seller', 'agency_user'];
const DEALER_SALES_MANAGEABLE_ROLES = ['seller'];
const LEGACY_READ_ONLY_ROLE = 'agency_user';

const BRAND_KEY_ALIASES = {
  'gac motor': 'gac',
  'gac motors': 'gac'
};

const normalizeBrandKey = (value) => {
  const key = (value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
  return BRAND_KEY_ALIASES[key] || key;
};

const EMPTY_AGENCY_FORM = {
  name: '',
  brand_id: '',
  address: '',
  city: '',
  street: '',
  exterior_number: '',
  interior_number: '',
  neighborhood: '',
  municipality: '',
  state: '',
  postal_code: '',
  country: 'Mexico',
  google_place_id: '',
  latitude: '',
  longitude: ''
};

const parseOptionalNumberField = (value) => {
  const text = String(value ?? '').trim();
  if (!text) return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : NaN;
};

const AGENCY_CREATION_ROLE_OPTIONS_GROUP = ['agency_general_manager', 'agency_sales_manager', 'seller'];
const AGENCY_CREATION_ROLE_OPTIONS_GENERAL = ['agency_sales_manager', 'seller'];

const getBrandInitials = (name) => {
  const tokens = String(name || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (tokens.length === 0) return '?';
  return tokens
    .slice(0, 2)
    .map((token) => token.charAt(0).toUpperCase())
    .join('');
};

const normalizeLogoUrl = (value) => {
  const url = String(value || '').trim();
  if (!url) return '';
  if (url.startsWith('//')) return `https:${url}`;
  return url;
};

export default function SettingsPage() {
  const { user, isAdmin } = useAuth();
  const userRole = user?.role || '';
  const isDealerGeneralManager = DEALER_GENERAL_EFFECTIVE_ROLES.includes(userRole);
  const isDealerSalesManager = DEALER_SALES_EFFECTIVE_ROLES.includes(userRole);
  const isDealerManager = DEALER_MANAGER_ROLES.includes(userRole);
  const canImportStructure = isAdmin || user?.role === 'group_admin';
  const canManageGroupStructure = isAdmin || user?.role === 'group_admin';
  const [groups, setGroups] = useState([]);
  const [brands, setBrands] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [sellers, setSellers] = useState([]);
  const [users, setUsers] = useState([]);
  const [catalogMakes, setCatalogMakes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [loadingCatalog, setLoadingCatalog] = useState(false);
  
  // Dialog states
  const [groupDialog, setGroupDialog] = useState(false);
  const [agencyDialog, setAgencyDialog] = useState(false);
  const [agencyUserDialog, setAgencyUserDialog] = useState(false);
  const [userDialog, setUserDialog] = useState(false);
  const [editUserDialog, setEditUserDialog] = useState(false);
  const [groupAdminDialog, setGroupAdminDialog] = useState(false);
  const [importDialog, setImportDialog] = useState(false);
  const [editingGroup, setEditingGroup] = useState(null);
  const [editingAgency, setEditingAgency] = useState(null);
  const [editingGroupAdmin, setEditingGroupAdmin] = useState(null);
  const [editingUser, setEditingUser] = useState(null);
  const [settingsTab, setSettingsTab] = useState('structure');
  const [usersGroupFilter, setUsersGroupFilter] = useState('all');
  const [usersRoleFilter, setUsersRoleFilter] = useState('all');
  const [groupBrandsToAssign, setGroupBrandsToAssign] = useState([]);
  const [groupBrandsToRemove, setGroupBrandsToRemove] = useState([]);
  const [importingStructure, setImportingStructure] = useState(false);
  const [deletingGroup, setDeletingGroup] = useState(false);
  const [groupDeleteError, setGroupDeleteError] = useState('');
  const [showCascadeDelete, setShowCascadeDelete] = useState(false);
  const [selectedGroupId, setSelectedGroupId] = useState('');
  const [groupSelectionInitialized, setGroupSelectionInitialized] = useState(false);
  const [showSelectedGroupDetails, setShowSelectedGroupDetails] = useState(true);
  const [expandedBrands, setExpandedBrands] = useState({});
  const [expandedAgencies, setExpandedAgencies] = useState({});
  const [expandedAgencyActivity, setExpandedAgencyActivity] = useState({});
  const [auditLogsByAgency, setAuditLogsByAgency] = useState({});
  const [loadingAuditAgencyId, setLoadingAuditAgencyId] = useState(null);
  const [failedBrandLogos, setFailedBrandLogos] = useState({});
  
  // Form states
  const [groupForm, setGroupForm] = useState({ name: '', description: '' });
  const [agencyForm, setAgencyForm] = useState(EMPTY_AGENCY_FORM);
  const [agencyUserForm, setAgencyUserForm] = useState({
    name: '',
    email: '',
    password: '',
    agency_id: '',
    role: 'agency_sales_manager',
    position: ''
  });
  const [groupAdminForm, setGroupAdminForm] = useState({
    name: '',
    position: '',
    role: 'group_admin',
    new_password: '',
    confirm_new_password: ''
  });
  const [userForm, setUserForm] = useState({
    name: '',
    position: '',
    email: '',
    password: '',
    role: 'group_admin',
    group_id: '',
    brand_id: '',
    agency_id: ''
  });
  const [editUserForm, setEditUserForm] = useState({
    name: '',
    position: '',
    role: 'group_admin',
    group_id: '',
    brand_id: '',
    agency_id: '',
    access_level: 'read',
    new_password: '',
    confirm_new_password: ''
  });
  const dealerAssignableRoles = isDealerGeneralManager
    ? DEALER_GENERAL_MANAGEABLE_ROLES
    : isDealerSalesManager
      ? DEALER_SALES_MANAGEABLE_ROLES
      : [];

  const canCreateUsers = isAdmin || user?.role === 'group_admin' || isDealerManager;

  const availableRoleOptions = isAdmin
    ? ROLES
    : user?.role === 'group_admin'
      ? ROLES.filter((role) => !APP_LEVEL_ROLES.includes(role.value))
      : isDealerGeneralManager
        ? ROLES.filter((role) => DEALER_GENERAL_MANAGEABLE_ROLES.includes(role.value))
        : isDealerSalesManager
          ? ROLES.filter((role) => DEALER_SALES_MANAGEABLE_ROLES.includes(role.value))
          : ROLES.filter((role) => !APP_LEVEL_ROLES.includes(role.value));

  const fetchCatalogMakes = useCallback(async () => {
    setLoadingCatalog(true);
    try {
      const res = await vehicleCatalogApi.getMakes();
      setCatalogMakes(res.data?.items || []);
    } catch (error) {
      setCatalogMakes([]);
      toast.error('No se pudo cargar el catalogo de marcas de Strapi');
    } finally {
      setLoadingCatalog(false);
    }
  }, []);

  const fetchData = useCallback(async () => {
    try {
      const [groupsRes, brandsRes, agenciesRes] = await Promise.all([
        groupsApi.getAll(),
        brandsApi.getAll(),
        agenciesApi.getAll()
      ]);
      setGroups(groupsRes.data);
      setBrands(brandsRes.data);
      setAgencies(agenciesRes.data);

      const sellersRes = await sellersApi.getAll();
      setSellers(sellersRes.data || []);

      if (isAdmin || user?.role === 'group_admin' || isDealerManager) {
        const usersRes = await usersApi.getAll();
        setUsers(usersRes.data);
      } else {
        setUsers([]);
      }
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [isAdmin, user?.role, isDealerManager]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    fetchCatalogMakes();
  }, [fetchCatalogMakes]);

  useEffect(() => {
    if (groups.length === 0) {
      if (selectedGroupId) setSelectedGroupId('');
      if (groupSelectionInitialized) setGroupSelectionInitialized(false);
      return;
    }

    if (selectedGroupId && groups.some((g) => g.id === selectedGroupId)) {
      return;
    }

    if (!groupSelectionInitialized) {
      if (user?.group_id && groups.some((g) => g.id === user.group_id)) {
        setSelectedGroupId(user.group_id);
      } else {
        setSelectedGroupId(groups[0].id);
      }
      setGroupSelectionInitialized(true);
      return;
    }

    if (selectedGroupId && !groups.some((g) => g.id === selectedGroupId)) {
      setSelectedGroupId('');
    }
  }, [groups, user?.group_id, selectedGroupId, groupSelectionInitialized]);

  useEffect(() => {
    if (!selectedGroupId) return;
    setShowSelectedGroupDetails(true);
    setExpandedBrands({});
    setExpandedAgencies({});
  }, [selectedGroupId]);

  useEffect(() => {
    if (!usersGroupFilter || usersGroupFilter === 'all') {
      if (!isAdmin && user?.group_id) {
        setUsersGroupFilter(user.group_id);
      }
    }
  }, [isAdmin, user?.group_id, usersGroupFilter]);

  const catalogLogoByName = useMemo(() => {
    const map = new Map();
    (catalogMakes || []).forEach((make) => {
      const key = normalizeBrandKey(make?.name);
      const logoUrl = normalizeLogoUrl(make?.logo_url);
      if (!key || !logoUrl || map.has(key)) return;
      map.set(key, logoUrl);
    });
    return map;
  }, [catalogMakes]);

  const getCatalogLogoUrl = useCallback(
    (brandName) => catalogLogoByName.get(normalizeBrandKey(brandName)) || '',
    [catalogLogoByName]
  );

  const handleSaveGroup = async (e) => {
    e.preventDefault();
    try {
      let targetGroupId = editingGroup?.id || null;
      let removedBrandsCount = 0;
      const addedBrandsCount = groupBrandsToAssign.length;

      if (editingGroup?.id) {
        await groupsApi.update(editingGroup.id, groupForm);
      } else {
        const created = await groupsApi.create(groupForm);
        targetGroupId = created?.data?.id || null;
      }

      if (editingGroup?.id && groupBrandsToRemove.length > 0) {
        const confirmedCascadeDelete = window.confirm(
          'Quitar una marca eliminará en cascada sus agencias, usuarios, vehículos, tasas, objetivos y comisiones relacionadas.\n\n¿Deseas continuar?'
        );
        if (confirmedCascadeDelete) {
          for (const brandId of groupBrandsToRemove) {
            // We remove the group-brand assignment and all dependent records for this brand.
            await brandsApi.delete(brandId, { cascade: true });
            removedBrandsCount += 1;
          }
        }
      }

      if (targetGroupId && groupBrandsToAssign.length > 0) {
        const selectedMakes = groupBrandsToAssign
          .map((name) => (name || '').trim())
          .filter(Boolean);

        await Promise.all(
          selectedMakes.map(async (makeName) => {
            const catalogLogoUrl = getCatalogLogoUrl(makeName);
            const existingInTargetGroup = brands.find(
              (brand) =>
                brand.group_id === targetGroupId &&
                normalizeBrandKey(brand.name) === normalizeBrandKey(makeName)
            );
            if (existingInTargetGroup) {
              const nextLogoUrl = normalizeLogoUrl(existingInTargetGroup.logo_url) || catalogLogoUrl || '';
              if (normalizeLogoUrl(existingInTargetGroup.logo_url) === nextLogoUrl) {
                return Promise.resolve();
              }
              return brandsApi.update(existingInTargetGroup.id, {
                name: existingInTargetGroup.name,
                group_id: targetGroupId,
                logo_url: nextLogoUrl
              });
            }
            return brandsApi.create({
              name: makeName,
              group_id: targetGroupId,
              logo_url: catalogLogoUrl || ''
            });
          })
        );
      }

      const assignedMessage = addedBrandsCount > 0
        ? ` y ${addedBrandsCount} marca(s) agregada(s)`
        : '';
      const removedMessage = removedBrandsCount > 0
        ? ` y ${removedBrandsCount} marca(s) eliminada(s)`
        : '';
      toast.success(`${editingGroup ? 'Grupo actualizado' : 'Grupo creado'} correctamente${assignedMessage}${removedMessage}`);

      setGroupDialog(false);
      setEditingGroup(null);
      setGroupBrandsToAssign([]);
      setGroupBrandsToRemove([]);
      setGroupForm({ name: '', description: '' });
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al guardar grupo');
    }
  };

  const openEditGroup = (group) => {
    setEditingGroup(group);
    setGroupBrandsToAssign([]);
    setGroupBrandsToRemove([]);
    setGroupDeleteError('');
    setShowCascadeDelete(false);
    setGroupForm({
      name: group.name || '',
      description: group.description || ''
    });
    setGroupDialog(true);
  };

  const handleDeleteGroup = async () => {
    if (!editingGroup?.id) return;
    setGroupDeleteError('');
    setShowCascadeDelete(false);

    const confirmed = window.confirm(
      `¿Estas seguro de borrar el grupo "${editingGroup.name}"?\n\n` +
      'Solo se puede borrar si no tiene marcas, agencias, usuarios u otros datos relacionados.'
    );
    if (!confirmed) return;

    try {
      setDeletingGroup(true);
      await groupsApi.delete(editingGroup.id);
      toast.success('Grupo eliminado correctamente');
      handleGroupDialogChange(false);
      fetchData();
    } catch (error) {
      const status = error.response?.status;
      const detail = error.response?.data?.detail;
      const message = typeof detail === 'string' ? detail : 'Error al borrar grupo';
      setGroupDeleteError(message);
      if (status === 409) {
        setShowCascadeDelete(true);
      }
      toast.error(message);
    } finally {
      setDeletingGroup(false);
    }
  };

  const handleCascadeDeleteGroup = async () => {
    if (!editingGroup?.id) return;

    const confirmed = window.confirm(
      `Esto borrará TODO el grupo "${editingGroup.name}" (marcas, agencias, vendedores y datos relacionados).\n\n` +
      'Esta acción no se puede deshacer. ¿Continuar?'
    );
    if (!confirmed) return;

    try {
      setDeletingGroup(true);
      await groupsApi.delete(editingGroup.id, { cascade: true });
      toast.success('Grupo y estructura eliminados correctamente');
      handleGroupDialogChange(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      const message = typeof detail === 'string' ? detail : 'Error al borrar en cascada';
      setGroupDeleteError(message);
      toast.error(message);
    } finally {
      setDeletingGroup(false);
    }
  };

  const handleGroupDialogChange = (open) => {
    setGroupDialog(open);
    if (!open) {
      setEditingGroup(null);
      setGroupBrandsToAssign([]);
      setGroupBrandsToRemove([]);
      setGroupDeleteError('');
      setShowCascadeDelete(false);
      setGroupForm({ name: '', description: '' });
    }
  };

  const toggleGroupBrandSelection = (brandName, checked) => {
    setGroupBrandsToAssign((prev) => {
      if (checked) {
        return prev.includes(brandName) ? prev : [...prev, brandName];
      }
      return prev.filter((name) => name !== brandName);
    });
  };

  const toggleGroupBrandRemoval = (brandId, checked) => {
    if (!brandId) return;
    setGroupBrandsToRemove((prev) => {
      if (checked) {
        return prev.includes(brandId) ? prev : [...prev, brandId];
      }
      return prev.filter((id) => id !== brandId);
    });
  };

  const handleSaveAgency = async (e) => {
    e.preventDefault();
    try {
      const latitude = parseOptionalNumberField(agencyForm.latitude);
      const longitude = parseOptionalNumberField(agencyForm.longitude);
      if (Number.isNaN(latitude) || Number.isNaN(longitude)) {
        toast.error('Latitud y longitud deben ser numeros validos');
        return;
      }

      const agencyPayload = {
        name: (agencyForm.name || '').trim(),
        brand_id: agencyForm.brand_id || editingAgency?.brand_id || '',
        address: (agencyForm.address || '').trim() || null,
        city: (agencyForm.city || '').trim() || null,
        street: (agencyForm.street || '').trim() || null,
        exterior_number: (agencyForm.exterior_number || '').trim() || null,
        interior_number: (agencyForm.interior_number || '').trim() || null,
        neighborhood: (agencyForm.neighborhood || '').trim() || null,
        municipality: (agencyForm.municipality || '').trim() || null,
        state: (agencyForm.state || '').trim() || null,
        postal_code: (agencyForm.postal_code || '').trim() || null,
        country: (agencyForm.country || '').trim() || null,
        google_place_id: (agencyForm.google_place_id || '').trim() || null,
        latitude,
        longitude
      };

      if (editingAgency?.id) {
        await agenciesApi.update(editingAgency.id, agencyPayload);
        toast.success('Agencia actualizada correctamente');
      } else {
        await agenciesApi.create(agencyPayload);
        toast.success('Agencia creada correctamente');
      }
      setAgencyDialog(false);
      setEditingAgency(null);
      setAgencyForm(EMPTY_AGENCY_FORM);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al guardar agencia');
    }
  };

  const openAgencyDialogForBrand = (brandId) => {
    setEditingAgency(null);
    setAgencyForm({ ...EMPTY_AGENCY_FORM, brand_id: brandId });
    setAgencyDialog(true);
  };

  const openEditAgencyDialog = (agency) => {
    setEditingAgency(agency);
    setAgencyForm({
      name: agency.name || '',
      brand_id: agency.brand_id || '',
      address: agency.address || '',
      city: agency.city || '',
      street: agency.street || '',
      exterior_number: agency.exterior_number || '',
      interior_number: agency.interior_number || '',
      neighborhood: agency.neighborhood || '',
      municipality: agency.municipality || '',
      state: agency.state || '',
      postal_code: agency.postal_code || '',
      country: agency.country || 'Mexico',
      google_place_id: agency.google_place_id || '',
      latitude: agency.latitude ?? '',
      longitude: agency.longitude ?? ''
    });
    setAgencyDialog(true);
  };

  const handleAgencyDialogChange = (open) => {
    setAgencyDialog(open);
    if (!open) {
      setEditingAgency(null);
      setAgencyForm(EMPTY_AGENCY_FORM);
    }
  };

  const openAgencyUserDialogForAgency = (agencyId) => {
    if (!isDealerGeneralManager && !canManageSelectedGroup) {
      toast.error('Solo gerencia general puede crear usuarios de dealer');
      return;
    }
    if (!canManageAgencyUsers(agencyId)) {
      toast.error('Solo puedes crear usuarios dentro de tu propia agencia');
      return;
    }
    setAgencyUserForm({
      name: '',
      email: '',
      password: '',
      agency_id: agencyId,
      role: canManageSelectedGroup ? 'agency_general_manager' : 'agency_sales_manager',
      position: ''
    });
    setAgencyUserDialog(true);
  };

  const handleAgencyUserDialogChange = (open) => {
    setAgencyUserDialog(open);
    if (!open) {
      setAgencyUserForm({
        name: '',
        email: '',
        password: '',
        agency_id: '',
        role: canManageSelectedGroup ? 'agency_general_manager' : 'agency_sales_manager',
        position: ''
      });
    }
  };

  const handleCreateAgencyUser = async (e) => {
    e.preventDefault();
    try {
      const agency = agencies.find((a) => a.id === agencyUserForm.agency_id);
      if (!agency) {
        toast.error('Selecciona una agencia valida');
        return;
      }

      const role = agencyUserForm.role;
      await authApi.register({
        name: agencyUserForm.name,
        position: (agencyUserForm.position || '').trim() || null,
        email: (agencyUserForm.email || '').trim(),
        password: agencyUserForm.password,
        role,
        group_id: agency.group_id,
        brand_id: agency.brand_id,
        agency_id: agency.id
      });

      toast.success(`Usuario de agencia creado (${getAgencyAccessLabel(role).toLowerCase()})`);
      handleAgencyUserDialogChange(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al crear usuario de agencia');
    }
  };

  const openCreateUserDialog = (defaults = {}) => {
    const defaultRole = defaults.role || (isAdmin ? 'group_admin' : 'seller');
    const defaultGroupId = isAdmin ? (defaults.group_id || '') : (user?.group_id || defaults.group_id || '');
    const defaultBrandId = defaults.brand_id || (isDealerManager ? (user?.brand_id || '') : '');
    const defaultAgencyId = defaults.agency_id || (isDealerManager ? (user?.agency_id || '') : '');
    setUserForm({
      name: '',
      position: '',
      email: '',
      password: '',
      role: defaultRole,
      group_id: defaultGroupId,
      brand_id: defaultBrandId,
      agency_id: defaultAgencyId
    });
    setUserDialog(true);
  };

  const openEditUserDialog = (userItem) => {
    if (!userItem?.id) return;
    if (userItem.id === user?.id && !isAdmin) {
      return;
    }

    if (isDealerManager && user?.agency_id && userItem.agency_id !== user.agency_id) {
      toast.error('Solo puedes editar usuarios de tu propia agencia');
      return;
    }
    if (isDealerSalesManager && userItem.role !== 'seller') {
      toast.error('Gerencia de ventas solo puede editar vendedores');
      return;
    }
    if (isDealerGeneralManager && !DEALER_GENERAL_MANAGEABLE_ROLES.includes(userItem.role)) {
      toast.error('Solo puedes editar usuarios operativos del dealer');
      return;
    }

    setEditingUser(userItem);
    setEditUserForm({
      name: userItem.name || '',
      position: userItem.position || '',
      role: userItem.role || 'agency_user',
      group_id: userItem.group_id || (isAdmin ? '' : (user?.group_id || '')),
      brand_id: userItem.brand_id || '',
      agency_id: userItem.agency_id || '',
      access_level: userItem.role === 'agency_sales_manager' ? 'write' : 'read',
      new_password: '',
      confirm_new_password: ''
    });
    setEditUserDialog(true);
  };

  const handleUserDialogChange = (open) => {
    setUserDialog(open);
    if (!open) {
      setUserForm({
        name: '',
        position: '',
        email: '',
        password: '',
        role: isAdmin ? 'group_admin' : 'seller',
        group_id: isAdmin ? '' : (user?.group_id || ''),
        brand_id: isDealerManager ? (user?.brand_id || '') : '',
        agency_id: isDealerManager ? (user?.agency_id || '') : ''
      });
    }
  };

  const handleEditUserDialogChange = (open) => {
    setEditUserDialog(open);
    if (!open) {
      setEditingUser(null);
      setEditUserForm({
        name: '',
        position: '',
        role: isAdmin ? 'group_admin' : 'seller',
        group_id: isAdmin ? '' : (user?.group_id || ''),
        brand_id: '',
        agency_id: '',
        access_level: 'read',
        new_password: '',
        confirm_new_password: ''
      });
    }
  };

  const handleCreateUser = async (e) => {
    e.preventDefault();
    const role = userForm.role;
    const effectiveGroupId = isAdmin ? userForm.group_id : (user?.group_id || userForm.group_id);
    if (isDealerManager) {
      if (!dealerAssignableRoles.includes(role)) {
        toast.error('No puedes asignar ese rol desde este dealer');
        return;
      }
      if (!user?.agency_id) {
        toast.error('Tu usuario no tiene dealer asignado');
        return;
      }
    }

    if (!APP_LEVEL_ROLES.includes(role) && !AGENCY_SCOPED_ROLES.includes(role) && !effectiveGroupId) {
      toast.error('Selecciona un grupo para el usuario');
      return;
    }

    if (BRAND_SCOPED_ROLES.includes(role) && !userForm.brand_id) {
      toast.error('Selecciona una marca para el rol seleccionado');
      return;
    }

    let payload = {
      name: (userForm.name || '').trim(),
      position: (userForm.position || '').trim() || null,
      email: (userForm.email || '').trim(),
      password: userForm.password,
      role
    };

    if (!APP_LEVEL_ROLES.includes(role)) {
      payload.group_id = effectiveGroupId;
    }

    if (BRAND_SCOPED_ROLES.includes(role) && userForm.brand_id) {
      payload.brand_id = userForm.brand_id;
    }

    if (AGENCY_SCOPED_ROLES.includes(role)) {
      const targetAgencyId = isDealerManager ? user?.agency_id : userForm.agency_id;
      if (!targetAgencyId) {
        toast.error('Selecciona una agencia para el rol seleccionado');
        return;
      }
      const agency = agencies.find((item) => item.id === targetAgencyId);
      if (!agency) {
        toast.error('Agencia no valida');
        return;
      }
      payload.group_id = agency.group_id;
      payload.brand_id = agency.brand_id;
      payload.agency_id = agency.id;
    }

    try {
      await authApi.register(payload);
      toast.success('Usuario creado correctamente');
      handleUserDialogChange(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al crear usuario');
    }
  };

  const handleSaveEditedUser = async (e) => {
    e.preventDefault();
    if (!editingUser?.id) return;

    let role = editUserForm.role;
    if ([LEGACY_READ_ONLY_ROLE, 'agency_sales_manager'].includes(role)) {
      role = editUserForm.access_level === 'write' ? 'agency_sales_manager' : LEGACY_READ_ONLY_ROLE;
    }
    const nextName = String(editUserForm.name || '').trim();
    const effectiveGroupId = isAdmin
      ? editUserForm.group_id
      : (user?.group_id || editUserForm.group_id || editingUser.group_id || '');

    if (!nextName) {
      toast.error('El nombre es obligatorio');
      return;
    }
    if (isDealerManager) {
      if (!dealerAssignableRoles.includes(role)) {
        toast.error('No puedes asignar ese rol desde este dealer');
        return;
      }
      if (!user?.agency_id || editingUser?.agency_id !== user.agency_id) {
        toast.error('Solo puedes editar usuarios de tu dealer');
        return;
      }
    }

    if (!APP_LEVEL_ROLES.includes(role) && !AGENCY_SCOPED_ROLES.includes(role) && !effectiveGroupId) {
      toast.error('Selecciona un grupo para el usuario');
      return;
    }

    if (BRAND_SCOPED_ROLES.includes(role) && !editUserForm.brand_id) {
      toast.error('Selecciona una marca para el rol seleccionado');
      return;
    }

    if (AGENCY_SCOPED_ROLES.includes(role) && !(isDealerManager ? user?.agency_id : editUserForm.agency_id)) {
      toast.error('Selecciona una agencia para el rol seleccionado');
      return;
    }

    const nextPassword = String(editUserForm.new_password || '');
    const nextPasswordConfirm = String(editUserForm.confirm_new_password || '');
    if (nextPassword) {
      if (nextPassword.length < 8) {
        toast.error('La nueva contraseña debe tener al menos 8 caracteres');
        return;
      }
      if (nextPassword !== nextPasswordConfirm) {
        toast.error('La confirmación de contraseña no coincide');
        return;
      }
    }

    const payload = {
      name: nextName,
      position: (editUserForm.position || '').trim() || null,
      role,
      group_id: APP_LEVEL_ROLES.includes(role) ? null : effectiveGroupId,
      brand_id: null,
      agency_id: null
    };

    if (BRAND_SCOPED_ROLES.includes(role)) {
      payload.brand_id = editUserForm.brand_id;
    }

    if (AGENCY_SCOPED_ROLES.includes(role)) {
      const targetAgencyId = isDealerManager ? user?.agency_id : editUserForm.agency_id;
      const agency = agencies.find((item) => item.id === targetAgencyId);
      if (!agency) {
        toast.error('Agencia no valida');
        return;
      }
      payload.group_id = agency.group_id;
      payload.brand_id = agency.brand_id;
      payload.agency_id = agency.id;
    }

    if (nextPassword) {
      payload.new_password = nextPassword;
    }

    try {
      await usersApi.update(editingUser.id, payload);
      toast.success('Usuario actualizado');
      handleEditUserDialogChange(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al editar usuario');
    }
  };

  const toggleAgencyActivityLogs = async (agencyId) => {
    const nextOpen = !expandedAgencyActivity[agencyId];
    setExpandedAgencyActivity((prev) => ({ ...prev, [agencyId]: nextOpen }));

    if (!nextOpen || auditLogsByAgency[agencyId]) return;

    try {
      setLoadingAuditAgencyId(agencyId);
      const res = await auditLogsApi.getAll({ agency_id: agencyId, limit: 30 });
      setAuditLogsByAgency((prev) => ({ ...prev, [agencyId]: res.data || [] }));
    } catch (error) {
      toast.error('No se pudo cargar el historial de cambios');
    } finally {
      setLoadingAuditAgencyId(null);
    }
  };

  const toggleBrandDetails = (brandId) => {
    setExpandedBrands((prev) => ({
      ...prev,
      [brandId]: !prev[brandId]
    }));
  };

  const toggleAgencyDetails = (agencyId) => {
    setExpandedAgencies((prev) => ({
      ...prev,
      [agencyId]: !prev[agencyId]
    }));
  };

  const handleUpdateUserRole = async (userId, newRole) => {
    if (isDealerSalesManager) {
      toast.error('Gerencia de ventas solo puede gestionar vendedores');
      return;
    }
    try {
      await usersApi.update(userId, { role: newRole });
      toast.success('Rol actualizado');
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al actualizar rol');
    }
  };

  const handleDeleteUser = async (userItem) => {
    if (!userItem?.id) return;
    if (userItem.id === user?.id) {
      toast.error('No puedes borrar tu propio usuario');
      return;
    }

    const confirmed = window.confirm(
      `¿Borrar usuario "${userItem.name}" (${userItem.email})? Esta acción no se puede deshacer.`
    );
    if (!confirmed) return;

    try {
      await usersApi.delete(userItem.id);
      toast.success('Usuario eliminado');
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al borrar usuario');
    }
  };

  const handleImportStructure = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setImportingStructure(true);
    try {
      const res = await organizationImportApi.import(file);
      const summary = res.data?.summary || {};
      const groupsSummary = summary.groups || {};
      const brandsSummary = summary.brands || {};
      const agenciesSummary = summary.agencies || {};
      const sellersSummary = summary.sellers || {};

      toast.success(
        `Importacion completa - Grupos C:${groupsSummary.created || 0}/A:${groupsSummary.updated || 0} - ` +
        `Marcas C:${brandsSummary.created || 0}/A:${brandsSummary.updated || 0} - ` +
        `Agencias C:${agenciesSummary.created || 0}/A:${agenciesSummary.updated || 0} - ` +
        `Vendedores C:${sellersSummary.created || 0}/A:${sellersSummary.updated || 0}`
      );

      const errors = res.data?.errors || [];
      if (errors.length > 0) {
        toast.warning(`Importación terminó con ${errors.length} observaciones`);
        // Keep a concise preview in console for troubleshooting.
        console.warn('Organization import warnings:', errors.slice(0, 20));
      }

      setImportDialog(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al importar estructura');
    } finally {
      setImportingStructure(false);
      e.target.value = '';
    }
  };

  const getGroupName = (id) => groups.find((g) => g.id === id)?.name || '-';

  const brandsByNormalizedName = new Map();
  brands.forEach((brand) => {
    const key = normalizeBrandKey(brand.name);
    if (!key) return;
    if (!brandsByNormalizedName.has(key)) {
      brandsByNormalizedName.set(key, []);
    }
    brandsByNormalizedName.get(key).push(brand);
  });

  const catalogBrandRows = (catalogMakes || []).map((make) => {
    const name = (make?.name || '').trim();
    const existingEntries = brandsByNormalizedName.get(normalizeBrandKey(name)) || [];
    const existingInEditingGroup = editingGroup
      ? existingEntries.find((entry) => entry.group_id === editingGroup.id)
      : null;
    const groupedIds = Array.from(new Set(existingEntries.map((entry) => entry.group_id).filter(Boolean)));
    const groupedNames = groupedIds.map((id) => getGroupName(id)).filter((label) => label !== '-');
    return {
      key: normalizeBrandKey(name),
      name,
      id: existingInEditingGroup?.id || null,
      group_ids: groupedIds,
      group_names: groupedNames,
      logo_url: normalizeLogoUrl(existingInEditingGroup?.logo_url) || normalizeLogoUrl(make?.logo_url)
    };
  }).filter((row) => row.name);

  const existingBrandRows = Array.from(brandsByNormalizedName.entries()).map(([key, brandRows]) => {
    const primary = brandRows[0] || {};
    const groupedIds = Array.from(new Set(brandRows.map((entry) => entry.group_id).filter(Boolean)));
    const groupedNames = groupedIds.map((id) => getGroupName(id)).filter((label) => label !== '-');
    const existingInEditingGroup = editingGroup
      ? brandRows.find((entry) => entry.group_id === editingGroup.id)
      : null;
    return {
      key,
      name: primary.name,
      id: existingInEditingGroup?.id || null,
      group_ids: groupedIds,
      group_names: groupedNames,
      logo_url: normalizeLogoUrl(existingInEditingGroup?.logo_url || primary.logo_url) || getCatalogLogoUrl(primary.name)
    };
  }).filter((row) => row.key && row.name);

  const sourceBrandRows = (() => {
    const mergedByKey = new Map();
    const mergeRow = (row) => {
      if (!row?.key || !row?.name) return;
      const existing = mergedByKey.get(row.key);
      if (!existing) {
        mergedByKey.set(row.key, {
          ...row,
          group_ids: Array.from(new Set(row.group_ids || [])),
          group_names: Array.from(new Set(row.group_names || []))
        });
        return;
      }
      mergedByKey.set(row.key, {
        ...existing,
        name: row.name || existing.name,
        id: existing.id || row.id || null,
        group_ids: Array.from(new Set([...(existing.group_ids || []), ...(row.group_ids || [])])),
        group_names: Array.from(new Set([...(existing.group_names || []), ...(row.group_names || [])])),
        logo_url: normalizeLogoUrl(row.logo_url) || normalizeLogoUrl(existing.logo_url)
      });
    };

    existingBrandRows.forEach(mergeRow);
    catalogBrandRows.forEach(mergeRow);

    return Array.from(mergedByKey.values()).sort((a, b) =>
      (a.name || '').localeCompare(b.name || '', 'es-MX')
    );
  })();

  const currentGroupBrands = editingGroup
    ? sourceBrandRows.filter((b) => (b.group_ids || []).includes(editingGroup.id))
    : [];
  const assignableBrands = editingGroup
    ? sourceBrandRows.filter((b) => !(b.group_ids || []).includes(editingGroup.id))
    : sourceBrandRows;
  const brandsByGroupId = brands.reduce((acc, brand) => {
    const key = brand.group_id || '__none__';
    if (!acc[key]) acc[key] = [];
    acc[key].push(brand);
    return acc;
  }, {});

  const agenciesByBrandId = agencies.reduce((acc, agency) => {
    const key = agency.brand_id || '__none__';
    if (!acc[key]) acc[key] = [];
    acc[key].push(agency);
    return acc;
  }, {});
  const brandNameById = useMemo(
    () => new Map(brands.map((brand) => [brand.id, brand.name])),
    [brands]
  );
  const agenciesForSelect = useMemo(() => (
    agencies
      .map((agency) => ({
        ...agency,
        display_name: `${agency.name}${brandNameById.get(agency.brand_id) ? ` · ${brandNameById.get(agency.brand_id)}` : ''}`
      }))
      .sort((a, b) => a.display_name.localeCompare(b.display_name, 'es-MX'))
  ), [agencies, brandNameById]);
  const agencyDisplayNameById = useMemo(
    () => new Map(agenciesForSelect.map((agency) => [agency.id, agency.display_name])),
    [agenciesForSelect]
  );

  const brandSummaryCards = useMemo(() => {
    const catalogNameByKey = new Map();
    (catalogMakes || []).forEach((make) => {
      const key = normalizeBrandKey(make?.name);
      const name = String(make?.name || '').trim();
      if (!key || !name || catalogNameByKey.has(key)) return;
      catalogNameByKey.set(key, name);
    });

    const summaryMap = new Map();
    brands.forEach((brand) => {
      const key = normalizeBrandKey(brand.name);
      if (!key) return;

      const existing = summaryMap.get(key) || {
        key,
        name: catalogNameByKey.get(key) || String(brand.name || '').trim() || key.toUpperCase(),
        logo_url: '',
        agency_ids: new Set(),
        group_ids: new Set()
      };

      const explicitLogoUrl = normalizeLogoUrl(brand.logo_url);
      const catalogLogoUrl = getCatalogLogoUrl(brand.name);
      if (!existing.logo_url && explicitLogoUrl) {
        existing.logo_url = explicitLogoUrl;
      } else if (!existing.logo_url && catalogLogoUrl) {
        existing.logo_url = catalogLogoUrl;
      }

      if (brand.group_id) {
        existing.group_ids.add(brand.group_id);
      }

      (agenciesByBrandId[brand.id] || []).forEach((agency) => {
        if (agency?.id) {
          existing.agency_ids.add(agency.id);
        }
      });

      summaryMap.set(key, existing);
    });

    return Array.from(summaryMap.values())
      .map((entry) => ({
        key: entry.key,
        name: entry.name,
        logo_url: entry.logo_url,
        agencies_count: entry.agency_ids.size,
        groups_count: entry.group_ids.size
      }))
      .sort((a, b) => (a.name || '').localeCompare(b.name || ''));
  }, [agenciesByBrandId, brands, catalogMakes, getCatalogLogoUrl]);

  const sellersByAgencyId = sellers.reduce((acc, seller) => {
    const key = seller.agency_id || '__none__';
    if (!acc[key]) acc[key] = [];
    acc[key].push(seller);
    return acc;
  }, {});

  const agencyUsersByAgencyId = users
    .filter((u) => u.agency_id && [
      'agency_admin',
      'agency_sales_manager',
      'agency_general_manager',
      'agency_commercial_manager',
      'agency_user'
    ].includes(u.role))
    .reduce((acc, agencyUser) => {
      const key = agencyUser.agency_id;
      if (!acc[key]) acc[key] = [];
      acc[key].push(agencyUser);
      return acc;
    }, {});

  const groupAdminsByGroupId = users
    .filter((u) => u.group_id && ['group_admin', 'group_finance_manager'].includes(u.role))
    .reduce((acc, groupAdmin) => {
      const key = groupAdmin.group_id;
      if (!acc[key]) acc[key] = [];
      acc[key].push(groupAdmin);
      return acc;
    }, {});

  const selectedGroup = groups.find((g) => g.id === selectedGroupId) || null;
  const selectedGroupAdmins = selectedGroup ? (groupAdminsByGroupId[selectedGroup.id] || []) : [];
  const selectedGroupHasEditableCorporateUsers = selectedGroupAdmins.some(
    (adminUser) => !(adminUser.id === user?.id && !isAdmin)
  );
  const selectedGroupBrands = selectedGroup ? (brandsByGroupId[selectedGroup.id] || []) : [];
  const selectedGroupAgencyCount = selectedGroupBrands.reduce(
    (total, brand) => total + ((agenciesByBrandId[brand.id] || []).length),
    0
  );
  const selectedGroupSellerCount = selectedGroupBrands.reduce(
    (total, brand) => total + (agenciesByBrandId[brand.id] || []).reduce(
      (agencyTotal, agency) => agencyTotal + ((sellersByAgencyId[agency.id] || []).length),
      0
    ),
    0
  );
  const selectedGroupAgencyIds = selectedGroupBrands.flatMap((brand) =>
    (agenciesByBrandId[brand.id] || []).map((agency) => agency.id)
  );
  const canManageSelectedGroup = !!selectedGroup && canManageGroupStructure && (
    isAdmin || !user?.group_id || user.group_id === selectedGroup.id
  );
  const agencyCreationRoleOptions = canManageSelectedGroup
    ? AGENCY_CREATION_ROLE_OPTIONS_GROUP
    : isDealerGeneralManager
      ? AGENCY_CREATION_ROLE_OPTIONS_GENERAL
      : DEALER_SALES_MANAGEABLE_ROLES;
  const canManageAgencyUsers = useCallback((agencyId) => {
    if (canManageSelectedGroup) return true;
    return isDealerManager && !!user?.agency_id && user.agency_id === agencyId;
  }, [canManageSelectedGroup, isDealerManager, user?.agency_id]);
  const selectedUserGroupId = isAdmin ? userForm.group_id : (user?.group_id || userForm.group_id);
  const userFormBrands = selectedUserGroupId ? (brandsByGroupId[selectedUserGroupId] || []) : [];
  const userFormAgencies = useMemo(() => {
    if (userForm.brand_id) {
      return agenciesByBrandId[userForm.brand_id] || [];
    }
    return agencies.filter((agency) => !selectedUserGroupId || agency.group_id === selectedUserGroupId);
  }, [agencies, agenciesByBrandId, selectedUserGroupId, userForm.brand_id]);
  const roleRequiresGroup = !APP_LEVEL_ROLES.includes(userForm.role) && !AGENCY_SCOPED_ROLES.includes(userForm.role);
  const roleRequiresBrand = BRAND_SCOPED_ROLES.includes(userForm.role);
  const roleRequiresAgency = AGENCY_SCOPED_ROLES.includes(userForm.role);
  const selectedEditUserGroupId = isAdmin
    ? editUserForm.group_id
    : (user?.group_id || editUserForm.group_id || editingUser?.group_id || '');
  const editUserFormBrands = selectedEditUserGroupId ? (brandsByGroupId[selectedEditUserGroupId] || []) : [];
  const editUserFormAgencies = useMemo(() => {
    if (editUserForm.brand_id) {
      return agenciesByBrandId[editUserForm.brand_id] || [];
    }
    return agencies.filter((agency) => !selectedEditUserGroupId || agency.group_id === selectedEditUserGroupId);
  }, [agencies, agenciesByBrandId, editUserForm.brand_id, selectedEditUserGroupId]);
  const userFormAgencyOptions = useMemo(
    () => userFormAgencies
      .map((agency) => ({
        ...agency,
        display_name: agencyDisplayNameById.get(agency.id) || agency.name
      }))
      .sort((a, b) => (a.display_name || '').localeCompare(b.display_name || '', 'es-MX')),
    [agencyDisplayNameById, userFormAgencies]
  );
  const editUserFormAgencyOptions = useMemo(
    () => editUserFormAgencies
      .map((agency) => ({
        ...agency,
        display_name: agencyDisplayNameById.get(agency.id) || agency.name
      }))
      .sort((a, b) => (a.display_name || '').localeCompare(b.display_name || '', 'es-MX')),
    [agencyDisplayNameById, editUserFormAgencies]
  );
  const editRoleRequiresGroup = !APP_LEVEL_ROLES.includes(editUserForm.role) && !AGENCY_SCOPED_ROLES.includes(editUserForm.role);
  const editRoleRequiresBrand = BRAND_SCOPED_ROLES.includes(editUserForm.role);
  const editRoleRequiresAgency = AGENCY_SCOPED_ROLES.includes(editUserForm.role);
  const editRoleSupportsAccess = ['agency_sales_manager', LEGACY_READ_ONLY_ROLE].includes(editUserForm.role);
  const groupAdminRoleOptions = availableRoleOptions.filter((role) =>
    ['group_admin', 'group_finance_manager', 'group_user'].includes(role.value)
  );

  const filteredUsers = users.filter((u) => {
    if (usersGroupFilter !== 'all' && u.group_id !== usersGroupFilter) return false;
    if (usersRoleFilter !== 'all' && u.role !== usersRoleFilter) return false;
    return true;
  });

  const handleGroupSelectorClick = (groupId) => {
    if (selectedGroupId === groupId) {
      setSelectedGroupId('');
      setShowSelectedGroupDetails(false);
      setExpandedBrands({});
      setExpandedAgencies({});
      setExpandedAgencyActivity({});
      return;
    }
    setSelectedGroupId(groupId);
    setShowSelectedGroupDetails(true);
  };

  const handleGroupAdminDialogChange = (open) => {
    setGroupAdminDialog(open);
    if (!open) {
      setEditingGroupAdmin(null);
      setGroupAdminForm({
        name: '',
        position: '',
        role: 'group_admin',
        new_password: '',
        confirm_new_password: ''
      });
    }
  };

  const openEditGroupAdminDialog = (adminUser) => {
    if (!adminUser) return;
    if (adminUser.id === user?.id && !isAdmin) {
      return;
    }
    setEditingGroupAdmin(adminUser);
    setGroupAdminForm({
      name: adminUser.name || '',
      position: adminUser.position || '',
      role: adminUser.role || 'group_admin',
      new_password: '',
      confirm_new_password: ''
    });
    setGroupAdminDialog(true);
  };

  const handleSaveGroupAdmin = async (e) => {
    e.preventDefault();
    if (!editingGroupAdmin?.id || !selectedGroup?.id) return;

    const nextPassword = String(groupAdminForm.new_password || '');
    const nextPasswordConfirm = String(groupAdminForm.confirm_new_password || '');

    if (nextPassword) {
      if (nextPassword.length < 8) {
        toast.error('La nueva contraseña debe tener al menos 8 caracteres');
        return;
      }
      if (nextPassword !== nextPasswordConfirm) {
        toast.error('La confirmación de contraseña no coincide');
        return;
      }
    }

    try {
      await usersApi.update(editingGroupAdmin.id, {
        name: groupAdminForm.name,
        position: (groupAdminForm.position || '').trim() || null,
        role: groupAdminForm.role,
        group_id: selectedGroup.id
      });
      if (nextPassword) {
        await authApi.resetPassword(editingGroupAdmin.email, nextPassword);
      }
      toast.success('Administrador de grupo actualizado');
      handleGroupAdminDialogChange(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al actualizar administrador');
    }
  };

  const toggleSelectedGroupBrands = () => {
    if (selectedGroupBrands.length === 0) return;
    setShowSelectedGroupDetails(true);
    const allBrandsExpanded = selectedGroupBrands.every((brand) => !!expandedBrands[brand.id]);
    const nextExpanded = !allBrandsExpanded;

    setExpandedBrands((prev) => {
      const next = { ...prev };
      selectedGroupBrands.forEach((brand) => {
        next[brand.id] = nextExpanded;
      });
      return next;
    });

    if (!nextExpanded) {
      setExpandedAgencies((prev) => {
        const next = { ...prev };
        selectedGroupAgencyIds.forEach((agencyId) => {
          next[agencyId] = false;
        });
        return next;
      });
      setExpandedAgencyActivity((prev) => {
        const next = { ...prev };
        selectedGroupAgencyIds.forEach((agencyId) => {
          next[agencyId] = false;
        });
        return next;
      });
    }
  };

  const toggleSelectedGroupAgencies = () => {
    if (selectedGroupBrands.length === 0) return;
    setShowSelectedGroupDetails(true);
    const allBrandsExpanded = selectedGroupBrands.every((brand) => !!expandedBrands[brand.id]);
    const nextExpanded = !allBrandsExpanded;

    setExpandedBrands((prev) => {
      const next = { ...prev };
      selectedGroupBrands.forEach((brand) => {
        next[brand.id] = nextExpanded;
      });
      return next;
    });

    if (!nextExpanded) {
      setExpandedAgencies((prev) => {
        const next = { ...prev };
        selectedGroupAgencyIds.forEach((agencyId) => {
          next[agencyId] = false;
        });
        return next;
      });
      setExpandedAgencyActivity((prev) => {
        const next = { ...prev };
        selectedGroupAgencyIds.forEach((agencyId) => {
          next[agencyId] = false;
        });
        return next;
      });
    }
  };

  const toggleSelectedGroupSellers = () => {
    if (selectedGroupAgencyIds.length === 0) return;
    setShowSelectedGroupDetails(true);

    setExpandedBrands((prev) => {
      const next = { ...prev };
      selectedGroupBrands.forEach((brand) => {
        next[brand.id] = true;
      });
      return next;
    });

    const allAgenciesExpanded = selectedGroupAgencyIds.every((agencyId) => !!expandedAgencies[agencyId]);
    const nextExpanded = !allAgenciesExpanded;

    setExpandedAgencies((prev) => {
      const next = { ...prev };
      selectedGroupAgencyIds.forEach((agencyId) => {
        next[agencyId] = nextExpanded;
      });
      return next;
    });
  };

  return (
    <div className="space-y-6" data-testid="settings-page">
      {/* Header */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Configuración
          </h1>
          <p className="text-muted-foreground">
            Gestiona grupos, marcas, agencias y usuarios
          </p>
        </div>
        {canImportStructure && (
          <Dialog open={importDialog} onOpenChange={setImportDialog}>
            <DialogTrigger asChild>
              <Button variant="outline" data-testid="import-organization-btn">
                <UploadSimple size={16} className="mr-2" />
                Importar Excel
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Importar Estructura Organizacional</DialogTitle>
              </DialogHeader>
              <div className="space-y-4">
                <p className="text-sm text-muted-foreground">
                  Sube un archivo Excel (.xlsx/.xls) con hojas opcionales: <strong>groups</strong>, <strong>brands</strong>, <strong>agencies</strong>, <strong>sellers</strong>.
                </p>
                <div className="text-sm text-muted-foreground space-y-1">
                  <p><strong>groups:</strong> name, description</p>
                  <p><strong>brands:</strong> name, group_id o group_name, logo_url</p>
                  <p><strong>agencies:</strong> name, brand_id o brand_name, city, address</p>
                  <p><strong>sellers:</strong> email, name, password, agency_id o agency_name, role</p>
                </div>
                <Input
                  type="file"
                  accept=".xlsx,.xls"
                  onChange={handleImportStructure}
                  disabled={importingStructure}
                  data-testid="import-organization-file-input"
                />
                {importingStructure && (
                  <p className="text-sm text-muted-foreground">Importando, por favor espera...</p>
                )}
              </div>
            </DialogContent>
          </Dialog>
        )}
      </div>

      <Tabs value={settingsTab} onValueChange={setSettingsTab} className="w-full">
        <TabsList>
          <TabsTrigger value="structure" data-testid="tab-structure">Estructura</TabsTrigger>
          {canCreateUsers && <TabsTrigger value="users" data-testid="tab-users">Usuarios</TabsTrigger>}
        </TabsList>

        <TabsContent value="structure" className="space-y-6 mt-6">
          {/* Groups */}
          <Card className="border-border/40">
            <CardHeader className="flex flex-row items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-md bg-[#002FA7]/10 flex items-center justify-center">
                  <Buildings size={20} weight="duotone" className="text-[#002FA7]" />
                </div>
                <div>
                  <CardTitle className="text-lg">Grupos</CardTitle>
                  <CardDescription>Administra aquí toda la jerarquía: marcas, agencias y vendedores</CardDescription>
                </div>
              </div>
              {canManageGroupStructure && (
                <Dialog open={groupDialog} onOpenChange={handleGroupDialogChange}>
                  {isAdmin && (
                    <DialogTrigger asChild>
                      <Button variant="outline" size="sm" data-testid="add-group-btn">
                        <Plus size={16} className="mr-1" /> Agregar
                      </Button>
                    </DialogTrigger>
                  )}
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>{editingGroup ? 'Editar Grupo' : 'Nuevo Grupo'}</DialogTitle>
                    </DialogHeader>
                    <form onSubmit={handleSaveGroup} className="space-y-4">
                      <div className="space-y-2">
                        <Label>Nombre</Label>
                        <Input
                          value={groupForm.name}
                          onChange={(e) => setGroupForm({ ...groupForm, name: e.target.value })}
                          required
                          data-testid="group-name-input"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Descripción</Label>
                        <Input
                          value={groupForm.description}
                          onChange={(e) => setGroupForm({ ...groupForm, description: e.target.value })}
                          data-testid="group-description-input"
                        />
                      </div>
                      {canManageGroupStructure && (
                        <div className="space-y-3 rounded-md border border-border/50 p-3">
                          {editingGroup && (
                            <div>
                              <Label className="text-sm">Marcas actualmente en este grupo</Label>
                              {currentGroupBrands.length === 0 ? (
                                <p className="text-xs text-muted-foreground mt-1">Este grupo aún no tiene marcas asignadas.</p>
                              ) : (
                                <div className="mt-2 max-h-36 overflow-y-auto space-y-2 pr-1">
                                  {currentGroupBrands.map((brand) => (
                                    <label key={brand.key} className="flex items-center justify-between gap-3 rounded-md border border-border/50 p-2">
                                      <div className="flex items-center gap-2">
                                        <Checkbox
                                          checked={groupBrandsToRemove.includes(brand.id)}
                                          onCheckedChange={(checked) => toggleGroupBrandRemoval(brand.id, checked === true)}
                                          data-testid={`remove-brand-checkbox-${brand.key}`}
                                        />
                                        <span className="text-sm font-medium">{brand.name}</span>
                                      </div>
                                      <span className="text-xs text-muted-foreground">
                                        {(agenciesByBrandId[brand.id] || []).length} agencias
                                      </span>
                                    </label>
                                  ))}
                                </div>
                              )}
                              {currentGroupBrands.length > 0 && (
                                <p className="text-xs text-muted-foreground mt-2">
                                  Selecciona aquí las marcas que quieras quitar del grupo.
                                </p>
                              )}
                            </div>
                          )}

                          <div className="space-y-2">
                            <Label className="text-sm">
                              {editingGroup ? 'Agregar marcas del catálogo Strapi a este grupo' : 'Agregar marcas del catálogo Strapi al nuevo grupo'}
                            </Label>
                            {loadingCatalog ? (
                              <p className="text-xs text-muted-foreground">Cargando catálogo de marcas...</p>
                            ) : assignableBrands.length === 0 ? (
                              <p className="text-xs text-muted-foreground">No hay marcas del catálogo pendientes por agregar.</p>
                            ) : (
                              <div className="max-h-40 overflow-y-auto space-y-2 pr-1">
                                {assignableBrands.map((brand) => (
                                  <label key={brand.key} className="flex items-center justify-between gap-3 rounded-md border border-border/50 p-2">
                                    <div className="flex items-center gap-2">
                                      <Checkbox
                                        checked={groupBrandsToAssign.includes(brand.name)}
                                        onCheckedChange={(checked) => toggleGroupBrandSelection(brand.name, checked === true)}
                                        data-testid={`assign-brand-checkbox-${brand.key}`}
                                      />
                                      <span className="text-sm font-medium">{brand.name}</span>
                                    </div>
                                    <span className="text-xs text-muted-foreground">
                                      {brand.group_ids?.length
                                        ? `Ya existe en ${brand.group_ids.length} grupo(s): ${brand.group_names.join(', ')}`
                                        : 'Sin asignar'}
                                    </span>
                                  </label>
                                ))}
                              </div>
                            )}
                            <p className="text-xs text-muted-foreground">
                              Las marcas seleccionadas se agregarán a este grupo sin afectar los otros grupos.
                            </p>
                          </div>
                        </div>
                      )}
                      {groupDeleteError && (
                        <p className="text-sm text-red-600" data-testid="group-delete-error">
                          {groupDeleteError}
                        </p>
                      )}
                      <div className={`flex gap-2 ${editingGroup && isAdmin ? 'justify-between' : 'justify-end'}`}>
                        {editingGroup && isAdmin && (
                          <div className="flex gap-2">
                            <Button
                              type="button"
                              variant="destructive"
                              onClick={handleDeleteGroup}
                              disabled={deletingGroup}
                              data-testid="delete-group-btn"
                            >
                              {deletingGroup ? 'Borrando...' : 'Borrar'}
                            </Button>
                            {showCascadeDelete && (
                              <Button
                                type="button"
                                className="bg-red-900 text-white hover:bg-red-900/90"
                                onClick={handleCascadeDeleteGroup}
                                disabled={deletingGroup}
                                data-testid="cascade-delete-group-btn"
                              >
                                {deletingGroup ? 'Borrando...' : 'Borrar con todo'}
                              </Button>
                            )}
                          </div>
                        )}
                        <div className="flex gap-2">
                          <Button type="button" variant="outline" onClick={() => handleGroupDialogChange(false)} disabled={deletingGroup}>
                            Cancelar
                          </Button>
                          <Button
                            type="submit"
                            className="bg-[#002FA7] hover:bg-[#002FA7]/90"
                            data-testid="save-group-btn"
                            disabled={deletingGroup}
                          >
                            {editingGroup ? 'Guardar Cambios' : 'Crear'}
                          </Button>
                        </div>
                      </div>
                    </form>
                  </DialogContent>
                </Dialog>
              )}
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="space-y-2">
                  {[1, 2].map((i) => <Skeleton key={i} className="h-12 w-full" />)}
                </div>
              ) : groups.length === 0 ? (
                <p className="text-center py-8 text-muted-foreground">No hay grupos configurados</p>
              ) : (
                <div className="space-y-4">
                  <div className="flex flex-wrap gap-2">
                    {groups.map((group) => (
                      <Button
                        key={group.id}
                        type="button"
                        size="sm"
                        variant={selectedGroupId === group.id ? 'default' : 'outline'}
                        className={selectedGroupId === group.id ? 'bg-[#002FA7] hover:bg-[#002FA7]/90' : ''}
                        onClick={() => handleGroupSelectorClick(group.id)}
                        data-testid={`group-selector-${group.id}`}
                      >
                        {group.name}
                      </Button>
                    ))}
                  </div>

                  {!selectedGroup ? (
                    <p className="text-sm text-muted-foreground">Selecciona un grupo para administrar su estructura.</p>
                  ) : (
                    <div className="rounded-md border border-border/60" data-testid={`group-${selectedGroup.id}`}>
                      <div className="flex flex-col gap-3 p-3 bg-muted/20 sm:flex-row sm:items-start sm:justify-between">
                        <div>
                          <div className="font-medium">{selectedGroup.name}</div>
                          {selectedGroup.description && (
                            <div className="text-sm text-muted-foreground">{selectedGroup.description}</div>
                          )}
                          <div className="mt-2 flex flex-wrap items-center gap-2">
                            <span className="text-xs text-muted-foreground">Equipo corporativo:</span>
                            {selectedGroupAdmins.length === 0 ? (
                              <Badge variant="outline">Sin admin</Badge>
                            ) : (
                              <>
                                {selectedGroupAdmins.map((adminUser) => (
                                  (adminUser.id === user?.id && !isAdmin) ? (
                                    <Badge key={adminUser.id} variant="secondary">
                                      {adminUser.name}{adminUser.position ? ` · ${adminUser.position}` : ''} (Tú)
                                    </Badge>
                                  ) : (
                                    <Button
                                      key={adminUser.id}
                                      type="button"
                                      variant="outline"
                                      size="sm"
                                      onClick={() => openEditGroupAdminDialog(adminUser)}
                                      data-testid={`edit-selected-group-admin-${adminUser.id}`}
                                    >
                                      {adminUser.name}{adminUser.position ? ` · ${adminUser.position}` : ''}
                                      <Pencil size={12} className="ml-1" />
                                    </Button>
                                  )
                                ))}
                                {selectedGroupHasEditableCorporateUsers && (
                                  <span className="text-xs text-muted-foreground">clic para editar</span>
                                )}
                              </>
                            )}
                          </div>
                        </div>
                        <div className="flex flex-wrap items-center gap-2">
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={toggleSelectedGroupBrands}
                            data-testid="selected-group-brands-toggle"
                          >
                            {selectedGroupBrands.length} marcas
                          </Button>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={toggleSelectedGroupAgencies}
                            data-testid="selected-group-agencies-toggle"
                          >
                            {selectedGroupAgencyCount} agencias
                          </Button>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={toggleSelectedGroupSellers}
                            data-testid="selected-group-sellers-toggle"
                          >
                            {selectedGroupSellerCount} vendedores
                          </Button>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={() => setShowSelectedGroupDetails((prev) => !prev)}
                            data-testid="toggle-group-details-btn"
                          >
                            {showSelectedGroupDetails ? 'Ocultar detalle' : 'Mostrar detalle'}
                          </Button>
                          {canCreateUsers && canManageSelectedGroup && (
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              onClick={() => openCreateUserDialog({ role: 'group_admin', group_id: selectedGroup.id })}
                              data-testid={`add-group-admin-${selectedGroup.id}`}
                            >
                              <UserPlus size={14} className="mr-1" /> Nuevo Admin
                            </Button>
                          )}
                          {canManageSelectedGroup && (
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() => openEditGroup(selectedGroup)}
                              data-testid={`edit-group-btn-${selectedGroup.id}`}
                            >
                              <Pencil size={16} />
                            </Button>
                          )}
                        </div>
                      </div>

                      {showSelectedGroupDetails && (
                        <div className="space-y-3 p-3">
                          {selectedGroupBrands.length === 0 ? (
                            <p className="text-sm text-muted-foreground">
                              Sin marcas asignadas. Usa editar grupo para asignar marcas del catálogo.
                            </p>
                          ) : (
                            <div className="grid gap-3 md:grid-cols-2 2xl:grid-cols-3">
                              {selectedGroupBrands.map((brand) => {
                                const brandAgencies = agenciesByBrandId[brand.id] || [];
                                const brandExpanded = !!expandedBrands[brand.id];
                                const logoUrl = normalizeLogoUrl(brand.logo_url) || getCatalogLogoUrl(brand.name);
                                const hasLogo = !!logoUrl && !failedBrandLogos[brand.id];
                                return (
                                  <div key={brand.id} className="rounded-md border border-border/50 p-3" data-testid={`brand-${brand.id}`}>
                                    <div className="flex items-start justify-between gap-3">
                                      <div className="flex items-start gap-3 min-w-0">
                                        <div className="h-12 w-12 shrink-0 rounded-md border border-border/60 bg-background flex items-center justify-center overflow-hidden">
                                          {hasLogo ? (
                                            <img
                                              src={logoUrl}
                                              alt={`Logo ${brand.name}`}
                                              className="h-10 w-10 object-contain"
                                              loading="lazy"
                                              onError={() => setFailedBrandLogos((prev) => ({ ...prev, [brand.id]: true }))}
                                            />
                                          ) : (
                                            <span className="text-xs font-semibold text-muted-foreground">
                                              {getBrandInitials(brand.name)}
                                            </span>
                                          )}
                                        </div>
                                        <div className="min-w-0">
                                          <div className="font-medium truncate">{brand.name}</div>
                                          <p className="text-xs text-muted-foreground">
                                            {brandAgencies.length} agencias en este grupo
                                          </p>
                                        </div>
                                      </div>
                                      <Button
                                        type="button"
                                        size="sm"
                                        variant="outline"
                                        onClick={() => toggleBrandDetails(brand.id)}
                                        data-testid={`toggle-brand-details-${brand.id}`}
                                      >
                                        {brandExpanded ? 'Ocultar' : 'Ver'}
                                      </Button>
                                    </div>

                                    <div className="mt-3 flex flex-wrap items-center gap-2">
                                      <Button
                                        type="button"
                                        size="sm"
                                        variant="outline"
                                        onClick={() => toggleBrandDetails(brand.id)}
                                        data-testid={`brand-agencies-count-${brand.id}`}
                                      >
                                        {brandAgencies.length} agencias
                                      </Button>
                                      {canManageSelectedGroup && (
                                        <Button
                                          type="button"
                                          size="sm"
                                          variant="ghost"
                                          onClick={() => openAgencyDialogForBrand(brand.id)}
                                          data-testid={`add-agency-for-brand-${brand.id}`}
                                        >
                                          <Plus size={14} className="mr-1" /> Agencia
                                        </Button>
                                      )}
                                    </div>

                                    {brandExpanded && (
                                      <div className="mt-2 space-y-2">
                                        {brandAgencies.length === 0 ? (
                                          <p className="text-sm text-muted-foreground">Sin agencias registradas.</p>
                                        ) : (
                                          brandAgencies.map((agency) => {
                                            const agencySellers = sellersByAgencyId[agency.id] || [];
                                            const agencyUsers = agencyUsersByAgencyId[agency.id] || [];
                                            const agencyGeneralUsers = agencyUsers.filter((u) => [
                                              'agency_admin',
                                              'agency_general_manager',
                                              'agency_commercial_manager'
                                            ].includes(u.role));
                                            const agencySalesUsers = agencyUsers.filter((u) => u.role === 'agency_sales_manager');
                                            const agencyReadUsers = agencyUsers.filter((u) => u.role === 'agency_user');
                                            const agencyExpanded = !!expandedAgencies[agency.id];
                                            const agencyActivityExpanded = !!expandedAgencyActivity[agency.id];
                                            const agencyAuditLogs = auditLogsByAgency[agency.id] || [];
                                            return (
                                              <div key={agency.id} className="rounded-md bg-muted/30 p-3" data-testid={`agency-${agency.id}`}>
                                                <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                                                  <div>
                                                    <div className="font-medium">{agency.name}</div>
                                                    <div className="text-sm text-muted-foreground">
                                                      {agency.city || agency.municipality || 'Sin ciudad'}
                                                      {agency.state ? `, ${agency.state}` : ''}
                                                      {agency.postal_code ? ` • CP ${agency.postal_code}` : ''}
                                                      {agency.address ? ` • ${agency.address}` : ''}
                                                    </div>
                                                    <div className="mt-1 flex flex-wrap items-center gap-2">
                                                      <Button
                                                        type="button"
                                                        variant="outline"
                                                        size="sm"
                                                        onClick={() => toggleAgencyDetails(agency.id)}
                                                        data-testid={`agency-write-users-count-${agency.id}`}
                                                      >
                                                        {agencyGeneralUsers.length} gerencia gral
                                                      </Button>
                                                      <Button
                                                        type="button"
                                                        variant="outline"
                                                        size="sm"
                                                        onClick={() => toggleAgencyDetails(agency.id)}
                                                        data-testid={`agency-sales-users-count-${agency.id}`}
                                                      >
                                                        {agencySalesUsers.length} gerencia ventas
                                                      </Button>
                                                      <Button
                                                        type="button"
                                                        variant="outline"
                                                        size="sm"
                                                        onClick={() => toggleAgencyDetails(agency.id)}
                                                        data-testid={`agency-read-users-count-${agency.id}`}
                                                      >
                                                        {agencyReadUsers.length} lectura legacy
                                                      </Button>
                                                    </div>
                                                  </div>
                                                  <div className="flex flex-wrap items-center gap-2">
                                                    <Button
                                                      type="button"
                                                      size="sm"
                                                      variant="outline"
                                                      onClick={() => toggleAgencyDetails(agency.id)}
                                                      data-testid={`toggle-agency-details-${agency.id}`}
                                                    >
                                                      {agencyExpanded ? 'Ocultar usuarios' : 'Mostrar usuarios'}
                                                    </Button>
                                                    <Button
                                                      type="button"
                                                      size="sm"
                                                      variant="outline"
                                                      onClick={() => toggleAgencyActivityLogs(agency.id)}
                                                      data-testid={`toggle-agency-activity-${agency.id}`}
                                                    >
                                                      {agencyActivityExpanded ? 'Ocultar historial' : 'Historial cambios'}
                                                    </Button>
                                                    {canManageAgencyUsers(agency.id) && (canManageSelectedGroup || isDealerGeneralManager) && (
                                                      <Button
                                                        type="button"
                                                        size="sm"
                                                        variant="ghost"
                                                        onClick={() => openAgencyUserDialogForAgency(agency.id)}
                                                        data-testid={`add-agency-user-${agency.id}`}
                                                      >
                                                        <UserPlus size={14} className="mr-1" /> Usuario
                                                      </Button>
                                                    )}
                                                    {canManageSelectedGroup && (
                                                      <Button
                                                        type="button"
                                                        size="sm"
                                                        variant="ghost"
                                                        onClick={() => openEditAgencyDialog(agency)}
                                                        data-testid={`edit-agency-${agency.id}`}
                                                      >
                                                        <Pencil size={14} className="mr-1" /> Editar
                                                      </Button>
                                                    )}
                                                  </div>
                                                </div>
                                                {agencyExpanded && (
                                                  <div className="mt-2 space-y-2">
                                                    <div>
                                                      <p className="text-xs uppercase tracking-wide text-muted-foreground">Usuarios de agencia</p>
                                                      <div className="mt-1 flex flex-wrap items-center gap-2">
                                                        {agencyUsers.length === 0 ? (
                                                          <span className="text-xs text-muted-foreground">Sin usuarios de agencia</span>
                                                        ) : (
                                                          agencyUsers.map((agencyUser) => (
                                                            <div key={agencyUser.id} className="inline-flex items-center gap-1 rounded-md border border-border/60 bg-background px-1 py-1">
                                                              <Badge variant="secondary">
                                                                {agencyUser.name}
                                                                {agencyUser.position ? ` · ${agencyUser.position}` : ''}
                                                                {' '}
                                                                ({getAgencyAccessLabel(agencyUser.role).toLowerCase()})
                                                              </Badge>
                                                              {canManageAgencyUsers(agency.id) && agencyUser.id !== user?.id && (canManageSelectedGroup || isDealerGeneralManager) && (
                                                                <>
                                                                  <Button
                                                                    type="button"
                                                                    size="sm"
                                                                    variant="outline"
                                                                    className="h-7 px-2 text-xs"
                                                                    onClick={() => openEditUserDialog(agencyUser)}
                                                                    data-testid={`edit-agency-user-${agencyUser.id}`}
                                                                  >
                                                                    <Pencil size={12} className="mr-1" /> Editar
                                                                  </Button>
                                                                  {['agency_sales_manager', LEGACY_READ_ONLY_ROLE].includes(agencyUser.role) && (
                                                                    <Button
                                                                      type="button"
                                                                      size="sm"
                                                                      variant="outline"
                                                                      className="h-7 px-2 text-xs"
                                                                      onClick={() => handleUpdateUserRole(
                                                                        agencyUser.id,
                                                                        agencyUser.role === 'agency_sales_manager' ? LEGACY_READ_ONLY_ROLE : 'agency_sales_manager'
                                                                      )}
                                                                      data-testid={`toggle-agency-user-role-${agencyUser.id}`}
                                                                    >
                                                                      {agencyUser.role === 'agency_sales_manager' ? 'Lectura' : 'Escritura'}
                                                                    </Button>
                                                                  )}
                                                                  <Button
                                                                    type="button"
                                                                    size="sm"
                                                                    variant="destructive"
                                                                    className="h-7 px-2 text-xs"
                                                                    onClick={() => handleDeleteUser(agencyUser)}
                                                                    data-testid={`delete-agency-user-${agencyUser.id}`}
                                                                  >
                                                                    <Trash size={12} className="mr-1" /> Borrar
                                                                  </Button>
                                                                </>
                                                              )}
                                                            </div>
                                                          ))
                                                        )}
                                                      </div>
                                                    </div>
                                                    <div>
                                                      <p className="text-xs uppercase tracking-wide text-muted-foreground">Vendedores</p>
                                                      <div className="mt-1 flex flex-wrap items-center gap-2">
                                                        {agencySellers.length === 0 ? (
                                                          <span className="text-xs text-muted-foreground">Sin vendedores</span>
                                                        ) : (
                                                          agencySellers.map((seller) => (
                                                            <div key={seller.id} className="inline-flex items-center gap-1 rounded-md border border-border/60 bg-background px-1 py-1">
                                                              <Badge variant="secondary">
                                                                {seller.name}
                                                                {seller.position ? ` · ${seller.position}` : ''}
                                                              </Badge>
                                                              {canManageAgencyUsers(agency.id) && seller.id !== user?.id && (
                                                                <>
                                                                  <Button
                                                                    type="button"
                                                                    size="sm"
                                                                    variant="outline"
                                                                    className="h-7 px-2 text-xs"
                                                                    onClick={() => openEditUserDialog(seller)}
                                                                  >
                                                                    <Pencil size={12} className="mr-1" /> Editar
                                                                  </Button>
                                                                  <Button
                                                                    type="button"
                                                                    size="sm"
                                                                    variant="destructive"
                                                                    className="h-7 px-2 text-xs"
                                                                    onClick={() => handleDeleteUser(seller)}
                                                                  >
                                                                    <Trash size={12} className="mr-1" /> Borrar
                                                                  </Button>
                                                                </>
                                                              )}
                                                            </div>
                                                          ))
                                                        )}
                                                      </div>
                                                    </div>
                                                  </div>
                                                )}
                                                {agencyActivityExpanded && (
                                                  <div className="mt-3 rounded-md border border-border/60 bg-background p-2">
                                                    <p className="text-xs uppercase tracking-wide text-muted-foreground mb-2">Cambios recientes (usuarios de escritura)</p>
                                                    {loadingAuditAgencyId === agency.id ? (
                                                      <p className="text-xs text-muted-foreground">Cargando historial...</p>
                                                    ) : agencyAuditLogs.length === 0 ? (
                                                      <p className="text-xs text-muted-foreground">Sin cambios registrados.</p>
                                                    ) : (
                                                      <div className="space-y-2">
                                                        {agencyAuditLogs.map((log) => (
                                                          <div key={log.id} className="rounded bg-muted/30 p-2 text-xs">
                                                            <div className="font-medium">
                                                              {new Date(log.created_at).toLocaleString('es-MX')} - {log.actor_name || log.actor_email || 'Usuario'} ({getRoleLabel(log.actor_role)})
                                                            </div>
                                                            <div className="text-muted-foreground">{log.action} · {log.entity_type}</div>
                                                          </div>
                                                        ))}
                                                      </div>
                                                    )}
                                                  </div>
                                                )}
                                              </div>
                                            );
                                          })
                                        )}
                                      </div>
                                    )}
                                  </div>
                                );
                              })}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Brands */}
          <Card className="border-border/40">
            <CardHeader className="flex flex-row items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-md bg-[#2A9D8F]/10 flex items-center justify-center">
                  <Factory size={20} weight="duotone" className="text-[#2A9D8F]" />
                </div>
                <div>
                  <CardTitle className="text-lg">Marcas</CardTitle>
                  <CardDescription>Solo informativo: marcas y agencias por marca</CardDescription>
                  <div className="mt-2 flex flex-wrap gap-2">
                    <Badge variant="outline">{groups.length} grupos</Badge>
                    <Badge variant="outline">{agencies.length} agencias</Badge>
                  </div>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="space-y-2">
                  {[1, 2, 3].map((i) => <Skeleton key={i} className="h-12 w-full" />)}
                </div>
              ) : brandSummaryCards.length === 0 ? (
                <p className="text-center py-8 text-muted-foreground">No hay marcas configuradas</p>
              ) : (
                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4">
                  {brandSummaryCards.map((brand) => {
                    const logoUrl = normalizeLogoUrl(brand.logo_url);
                    return (
                      <div key={brand.key} className="p-3 rounded-md border border-border/60 bg-muted/30" data-testid={`brand-summary-${brand.key}`}>
                        <div className="flex items-start gap-3">
                          <div className="h-12 w-12 shrink-0 rounded-md border border-border/60 bg-background flex items-center justify-center overflow-hidden">
                            {logoUrl && !failedBrandLogos[brand.key] ? (
                              <img
                                src={logoUrl}
                                alt={`Logo ${brand.name}`}
                                className="h-10 w-10 object-contain"
                                loading="lazy"
                                onError={() => setFailedBrandLogos((prev) => ({ ...prev, [brand.key]: true }))}
                              />
                            ) : (
                              <span className="text-xs font-semibold text-muted-foreground">
                                {getBrandInitials(brand.name)}
                              </span>
                            )}
                          </div>
                          <div className="min-w-0 flex-1">
                            <div className="font-medium truncate">{brand.name}</div>
                            <div className="mt-2 flex flex-wrap gap-2">
                              <Badge variant="outline">{brand.agencies_count} a</Badge>
                              <Badge variant="outline">{brand.groups_count} g</Badge>
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </CardContent>
          </Card>

          {canManageGroupStructure && (
            <>
              <Dialog open={groupAdminDialog} onOpenChange={handleGroupAdminDialogChange}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Editar Admin de Grupo</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleSaveGroupAdmin} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Grupo</Label>
                      <Input value={selectedGroup?.name || ''} disabled />
                    </div>
                    <div className="space-y-2">
                      <Label>Email</Label>
                      <Input value={editingGroupAdmin?.email || ''} disabled />
                    </div>
                    <div className="space-y-2">
                      <Label>Nombre</Label>
                      <Input
                        value={groupAdminForm.name}
                        onChange={(e) => setGroupAdminForm((prev) => ({ ...prev, name: e.target.value }))}
                        required
                        data-testid="edit-group-admin-name-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Puesto (opcional)</Label>
                      <Input
                        value={groupAdminForm.position}
                        onChange={(e) => setGroupAdminForm((prev) => ({ ...prev, position: e.target.value }))}
                        placeholder="Ej. Director Regional"
                        data-testid="edit-group-admin-position-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Rol</Label>
                      <Select
                        value={groupAdminForm.role}
                        onValueChange={(value) => setGroupAdminForm((prev) => ({ ...prev, role: value }))}
                      >
                        <SelectTrigger data-testid="edit-group-admin-role-select">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {groupAdminRoleOptions.map((role) => (
                            <SelectItem key={role.value} value={role.value}>
                              {role.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Nueva contraseña (opcional)</Label>
                      <Input
                        type="password"
                        value={groupAdminForm.new_password}
                        onChange={(e) => setGroupAdminForm((prev) => ({ ...prev, new_password: e.target.value }))}
                        data-testid="edit-group-admin-password-input"
                      />
                      <p className="text-xs text-muted-foreground">
                        Déjalo vacío si no quieres cambiarla.
                      </p>
                    </div>
                    <div className="space-y-2">
                      <Label>Confirmar nueva contraseña</Label>
                      <Input
                        type="password"
                        value={groupAdminForm.confirm_new_password}
                        onChange={(e) => setGroupAdminForm((prev) => ({ ...prev, confirm_new_password: e.target.value }))}
                        data-testid="edit-group-admin-password-confirm-input"
                      />
                    </div>
                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => handleGroupAdminDialogChange(false)}>
                        Cancelar
                      </Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-group-admin-btn">
                        Guardar Cambios
                      </Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>

              <Dialog open={agencyDialog} onOpenChange={handleAgencyDialogChange}>
                <DialogContent className="sm:max-w-3xl max-h-[85vh] overflow-y-auto">
                  <DialogHeader>
                    <DialogTitle>{editingAgency ? 'Editar Agencia' : 'Nueva Agencia'}</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleSaveAgency} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Marca</Label>
                      <Select
                        value={agencyForm.brand_id}
                        onValueChange={(v) => setAgencyForm({ ...agencyForm, brand_id: v })}
                        required
                        disabled={!!editingAgency}
                      >
                        <SelectTrigger data-testid="agency-brand-select">
                          <SelectValue placeholder="Seleccionar marca" />
                        </SelectTrigger>
                        <SelectContent>
                          {brands.map((b) => (
                            <SelectItem key={b.id} value={b.id}>{b.name}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Nombre</Label>
                      <Input
                        value={agencyForm.name}
                        onChange={(e) => setAgencyForm({ ...agencyForm, name: e.target.value })}
                        required
                        data-testid="agency-name-input"
                      />
                    </div>
                    <div className="rounded-md border border-border/60 p-3 space-y-3">
                      <p className="text-sm font-medium">Direccion estilo Google</p>
                      <div className="grid gap-3 md:grid-cols-2">
                        <div className="space-y-2">
                          <Label>Calle (route)</Label>
                          <Input
                            value={agencyForm.street}
                            onChange={(e) => setAgencyForm({ ...agencyForm, street: e.target.value })}
                            data-testid="agency-street-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Numero exterior (street_number)</Label>
                          <Input
                            value={agencyForm.exterior_number}
                            onChange={(e) => setAgencyForm({ ...agencyForm, exterior_number: e.target.value })}
                            data-testid="agency-exterior-number-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Numero interior</Label>
                          <Input
                            value={agencyForm.interior_number}
                            onChange={(e) => setAgencyForm({ ...agencyForm, interior_number: e.target.value })}
                            data-testid="agency-interior-number-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Colonia (sublocality)</Label>
                          <Input
                            value={agencyForm.neighborhood}
                            onChange={(e) => setAgencyForm({ ...agencyForm, neighborhood: e.target.value })}
                            data-testid="agency-neighborhood-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Ciudad / Localidad (locality)</Label>
                          <Input
                            value={agencyForm.city}
                            onChange={(e) => setAgencyForm({ ...agencyForm, city: e.target.value })}
                            data-testid="agency-city-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Municipio / Alcaldia</Label>
                          <Input
                            value={agencyForm.municipality}
                            onChange={(e) => setAgencyForm({ ...agencyForm, municipality: e.target.value })}
                            data-testid="agency-municipality-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Estado (administrative_area_level_1)</Label>
                          <Input
                            value={agencyForm.state}
                            onChange={(e) => setAgencyForm({ ...agencyForm, state: e.target.value })}
                            data-testid="agency-state-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Codigo Postal (postal_code)</Label>
                          <Input
                            value={agencyForm.postal_code}
                            onChange={(e) => setAgencyForm({ ...agencyForm, postal_code: e.target.value })}
                            data-testid="agency-postal-code-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Pais (country)</Label>
                          <Input
                            value={agencyForm.country}
                            onChange={(e) => setAgencyForm({ ...agencyForm, country: e.target.value })}
                            data-testid="agency-country-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Google Place ID</Label>
                          <Input
                            value={agencyForm.google_place_id}
                            onChange={(e) => setAgencyForm({ ...agencyForm, google_place_id: e.target.value })}
                            data-testid="agency-place-id-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Latitud</Label>
                          <Input
                            value={agencyForm.latitude}
                            onChange={(e) => setAgencyForm({ ...agencyForm, latitude: e.target.value })}
                            data-testid="agency-latitude-input"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>Longitud</Label>
                          <Input
                            value={agencyForm.longitude}
                            onChange={(e) => setAgencyForm({ ...agencyForm, longitude: e.target.value })}
                            data-testid="agency-longitude-input"
                          />
                        </div>
                      </div>
                      <div className="space-y-2">
                        <Label>Direccion completa (formatted_address)</Label>
                        <Input
                          value={agencyForm.address}
                          onChange={(e) => setAgencyForm({ ...agencyForm, address: e.target.value })}
                          data-testid="agency-address-input"
                        />
                      </div>
                    </div>
                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => handleAgencyDialogChange(false)}>Cancelar</Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-agency-btn">
                        {editingAgency ? 'Guardar Cambios' : 'Crear'}
                      </Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>
            </>
          )}

          {canCreateUsers && (
            <>
              <Dialog open={agencyUserDialog} onOpenChange={handleAgencyUserDialogChange}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Nuevo Usuario de Agencia</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleCreateAgencyUser} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Agencia</Label>
                      <Select value={agencyUserForm.agency_id} onValueChange={(v) => setAgencyUserForm({ ...agencyUserForm, agency_id: v })} required>
                        <SelectTrigger data-testid="agency-user-agency-select">
                          <SelectValue placeholder="Seleccionar agencia" />
                        </SelectTrigger>
                        <SelectContent>
                          {agenciesForSelect.map((a) => (
                            <SelectItem key={a.id} value={a.id}>{a.display_name}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Rol</Label>
                      <Select
                        value={agencyUserForm.role}
                        onValueChange={(v) => setAgencyUserForm({ ...agencyUserForm, role: v })}
                      >
                        <SelectTrigger data-testid="agency-user-role-select">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {agencyCreationRoleOptions.map((roleValue) => (
                            <SelectItem key={roleValue} value={roleValue}>
                              {getRoleLabel(roleValue)}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Nombre</Label>
                      <Input
                        value={agencyUserForm.name}
                        onChange={(e) => setAgencyUserForm({ ...agencyUserForm, name: e.target.value })}
                        required
                        data-testid="agency-user-name-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Puesto (opcional)</Label>
                      <Input
                        value={agencyUserForm.position}
                        onChange={(e) => setAgencyUserForm({ ...agencyUserForm, position: e.target.value })}
                        placeholder="Ej. Asesor Comercial"
                        data-testid="agency-user-position-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Email</Label>
                      <Input
                        type="email"
                        value={agencyUserForm.email}
                        onChange={(e) => setAgencyUserForm({ ...agencyUserForm, email: e.target.value })}
                        required
                        data-testid="agency-user-email-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Contraseña temporal</Label>
                      <Input
                        type="password"
                        value={agencyUserForm.password}
                        onChange={(e) => setAgencyUserForm({ ...agencyUserForm, password: e.target.value })}
                        required
                        data-testid="agency-user-password-input"
                      />
                    </div>
                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => handleAgencyUserDialogChange(false)}>Cancelar</Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-agency-user-btn">
                        Crear
                      </Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>

              <Dialog open={userDialog} onOpenChange={handleUserDialogChange}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Nuevo Usuario</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleCreateUser} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Rol</Label>
                      <Select
                        value={userForm.role}
                        onValueChange={(value) => {
                          setUserForm((prev) => {
                            const next = { ...prev, role: value };
                            if (APP_LEVEL_ROLES.includes(value)) {
                              next.group_id = '';
                              next.brand_id = '';
                              next.agency_id = '';
                              return next;
                            }
                            if (!isAdmin) {
                              next.group_id = user?.group_id || prev.group_id;
                            }
                            if (!BRAND_SCOPED_ROLES.includes(value) && !AGENCY_SCOPED_ROLES.includes(value)) {
                              next.brand_id = '';
                            }
                            if (!AGENCY_SCOPED_ROLES.includes(value)) {
                              next.agency_id = '';
                            }
                            return next;
                          });
                        }}
                      >
                        <SelectTrigger data-testid="create-user-role-select">
                          <SelectValue placeholder="Selecciona rol" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableRoleOptions.map((role) => (
                            <SelectItem key={role.value} value={role.value}>
                              {role.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    {roleRequiresGroup && (
                      <div className="space-y-2">
                        <Label>Grupo</Label>
                        <Select
                          value={selectedUserGroupId || ''}
                          onValueChange={(value) => setUserForm((prev) => ({
                            ...prev,
                            group_id: value,
                            brand_id: '',
                            agency_id: ''
                          }))}
                          disabled={!isAdmin}
                        >
                          <SelectTrigger data-testid="create-user-group-select">
                            <SelectValue placeholder="Selecciona grupo" />
                          </SelectTrigger>
                          <SelectContent>
                            {(isAdmin ? groups : groups.filter((g) => g.id === user?.group_id)).map((group) => (
                              <SelectItem key={group.id} value={group.id}>
                                {group.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    {(roleRequiresBrand || roleRequiresAgency) && (
                      <div className="space-y-2">
                        <Label>Marca</Label>
                        <Select
                          value={userForm.brand_id}
                          onValueChange={(value) => setUserForm((prev) => ({
                            ...prev,
                            brand_id: value,
                            agency_id: ''
                          }))}
                        >
                          <SelectTrigger data-testid="create-user-brand-select">
                            <SelectValue placeholder="Selecciona marca" />
                          </SelectTrigger>
                          <SelectContent>
                            {userFormBrands.map((brand) => (
                              <SelectItem key={brand.id} value={brand.id}>
                                {brand.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    {roleRequiresAgency && (
                      <div className="space-y-2">
                        <Label>Agencia</Label>
                        <Select
                          value={userForm.agency_id}
                          onValueChange={(value) => setUserForm((prev) => ({ ...prev, agency_id: value }))}
                        >
                          <SelectTrigger data-testid="create-user-agency-select">
                            <SelectValue placeholder="Selecciona agencia" />
                          </SelectTrigger>
                          <SelectContent>
                            {userFormAgencyOptions.map((agency) => (
                              <SelectItem key={agency.id} value={agency.id}>
                                {agency.display_name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    <div className="space-y-2">
                      <Label>Nombre</Label>
                      <Input
                        value={userForm.name}
                        onChange={(e) => setUserForm({ ...userForm, name: e.target.value })}
                        required
                        data-testid="create-user-name-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Puesto (opcional)</Label>
                      <Input
                        value={userForm.position}
                        onChange={(e) => setUserForm({ ...userForm, position: e.target.value })}
                        placeholder="Ej. Gerente de Operaciones"
                        data-testid="create-user-position-input"
                      />
                    </div>

                    <div className="space-y-2">
                      <Label>Email</Label>
                      <Input
                        type="email"
                        value={userForm.email}
                        onChange={(e) => setUserForm({ ...userForm, email: e.target.value })}
                        required
                        data-testid="create-user-email-input"
                      />
                    </div>

                    <div className="space-y-2">
                      <Label>Contraseña temporal</Label>
                      <Input
                        type="password"
                        value={userForm.password}
                        onChange={(e) => setUserForm({ ...userForm, password: e.target.value })}
                        required
                        data-testid="create-user-password-input"
                      />
                    </div>

                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => handleUserDialogChange(false)}>Cancelar</Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-user-btn">
                        Crear
                      </Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>

              <Dialog open={editUserDialog} onOpenChange={handleEditUserDialogChange}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Editar Usuario</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleSaveEditedUser} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Email</Label>
                      <Input
                        type="email"
                        value={editingUser?.email || ''}
                        disabled
                        data-testid="edit-user-email-input"
                      />
                    </div>

                    <div className="space-y-2">
                      <Label>Rol</Label>
                      <Select
                        value={editUserForm.role}
                        onValueChange={(value) => {
                          setEditUserForm((prev) => {
                            const next = { ...prev, role: value };
                            if (['agency_sales_manager', LEGACY_READ_ONLY_ROLE].includes(value)) {
                              next.access_level = value === 'agency_sales_manager' ? 'write' : 'read';
                            }
                            if (APP_LEVEL_ROLES.includes(value)) {
                              next.group_id = '';
                              next.brand_id = '';
                              next.agency_id = '';
                              return next;
                            }
                            if (!isAdmin) {
                              next.group_id = user?.group_id || prev.group_id || editingUser?.group_id || '';
                            }
                            if (!BRAND_SCOPED_ROLES.includes(value) && !AGENCY_SCOPED_ROLES.includes(value)) {
                              next.brand_id = '';
                            }
                            if (!AGENCY_SCOPED_ROLES.includes(value)) {
                              next.agency_id = '';
                            }
                            if (!['agency_sales_manager', LEGACY_READ_ONLY_ROLE].includes(value)) {
                              next.access_level = 'read';
                            }
                            return next;
                          });
                        }}
                      >
                        <SelectTrigger data-testid="edit-user-role-select">
                          <SelectValue placeholder="Selecciona rol" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableRoleOptions.map((role) => (
                            <SelectItem key={role.value} value={role.value}>
                              {role.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    {editRoleSupportsAccess && (
                      <div className="space-y-2">
                        <Label>Permiso de Agencia</Label>
                        <Select
                          value={editUserForm.access_level}
                          onValueChange={(value) => setEditUserForm((prev) => ({
                            ...prev,
                            access_level: value === 'write' ? 'write' : 'read',
                            role: value === 'write' ? 'agency_sales_manager' : LEGACY_READ_ONLY_ROLE
                          }))}
                        >
                          <SelectTrigger data-testid="edit-user-access-select">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="read">Solo lectura</SelectItem>
                            <SelectItem value="write">Escritura</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    {editRoleRequiresGroup && (
                      <div className="space-y-2">
                        <Label>Grupo</Label>
                        <Select
                          value={selectedEditUserGroupId || ''}
                          onValueChange={(value) => setEditUserForm((prev) => ({
                            ...prev,
                            group_id: value,
                            brand_id: '',
                            agency_id: ''
                          }))}
                          disabled={!isAdmin}
                        >
                          <SelectTrigger data-testid="edit-user-group-select">
                            <SelectValue placeholder="Selecciona grupo" />
                          </SelectTrigger>
                          <SelectContent>
                            {(isAdmin ? groups : groups.filter((g) => g.id === user?.group_id)).map((group) => (
                              <SelectItem key={group.id} value={group.id}>
                                {group.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    {(editRoleRequiresBrand || editRoleRequiresAgency) && (
                      <div className="space-y-2">
                        <Label>Marca</Label>
                        <Select
                          value={editUserForm.brand_id}
                          onValueChange={(value) => setEditUserForm((prev) => ({
                            ...prev,
                            brand_id: value,
                            agency_id: ''
                          }))}
                        >
                          <SelectTrigger data-testid="edit-user-brand-select">
                            <SelectValue placeholder="Selecciona marca" />
                          </SelectTrigger>
                          <SelectContent>
                            {editUserFormBrands.map((brand) => (
                              <SelectItem key={brand.id} value={brand.id}>
                                {brand.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    {editRoleRequiresAgency && (
                      <div className="space-y-2">
                        <Label>Agencia</Label>
                        <Select
                          value={editUserForm.agency_id}
                          onValueChange={(value) => setEditUserForm((prev) => ({ ...prev, agency_id: value }))}
                        >
                          <SelectTrigger data-testid="edit-user-agency-select">
                            <SelectValue placeholder="Selecciona agencia" />
                          </SelectTrigger>
                          <SelectContent>
                            {editUserFormAgencyOptions.map((agency) => (
                              <SelectItem key={agency.id} value={agency.id}>
                                {agency.display_name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    <div className="space-y-2">
                      <Label>Nombre</Label>
                      <Input
                        value={editUserForm.name}
                        onChange={(e) => setEditUserForm((prev) => ({ ...prev, name: e.target.value }))}
                        required
                        data-testid="edit-user-name-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Puesto (opcional)</Label>
                      <Input
                        value={editUserForm.position}
                        onChange={(e) => setEditUserForm((prev) => ({ ...prev, position: e.target.value }))}
                        placeholder="Ej. Coordinador de Ventas"
                        data-testid="edit-user-position-input"
                      />
                    </div>

                    <div className="space-y-2">
                      <Label>Nueva contraseña (opcional)</Label>
                      <Input
                        type="password"
                        value={editUserForm.new_password}
                        onChange={(e) => setEditUserForm((prev) => ({ ...prev, new_password: e.target.value }))}
                        data-testid="edit-user-password-input"
                      />
                    </div>

                    <div className="space-y-2">
                      <Label>Confirmar nueva contraseña</Label>
                      <Input
                        type="password"
                        value={editUserForm.confirm_new_password}
                        onChange={(e) => setEditUserForm((prev) => ({ ...prev, confirm_new_password: e.target.value }))}
                        data-testid="edit-user-password-confirm-input"
                      />
                    </div>

                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => handleEditUserDialogChange(false)}>
                        Cancelar
                      </Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-edit-user-btn">
                        Guardar Cambios
                      </Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>

            </>
          )}
        </TabsContent>

        {canCreateUsers && (
          <TabsContent value="users" className="mt-6">
            <Card className="border-border/40">
              <CardHeader className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-md bg-[#002FA7]/10 flex items-center justify-center">
                    <Users size={20} weight="duotone" className="text-[#002FA7]" />
                  </div>
                  <div>
                    <CardTitle className="text-lg">Usuarios</CardTitle>
                    <CardDescription>Gestiona roles y permisos de usuarios</CardDescription>
                  </div>
                </div>
                {canCreateUsers && (
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => openCreateUserDialog()}
                    data-testid="add-user-btn"
                  >
                    <UserPlus size={16} className="mr-2" />
                    Agregar Usuario
                  </Button>
                )}
              </CardHeader>
              <CardContent>
                <div className="mb-4 grid gap-3 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label>Filtrar por Grupo</Label>
                    <Select
                      value={usersGroupFilter}
                      onValueChange={setUsersGroupFilter}
                    >
                      <SelectTrigger data-testid="users-group-filter-select">
                        <SelectValue placeholder="Todos los grupos" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">Todos los grupos</SelectItem>
                        {(isAdmin ? groups : groups.filter((g) => g.id === user?.group_id)).map((group) => (
                          <SelectItem key={group.id} value={group.id}>
                            {group.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Filtrar por Rol</Label>
                    <Select
                      value={usersRoleFilter}
                      onValueChange={setUsersRoleFilter}
                    >
                      <SelectTrigger data-testid="users-role-filter-select">
                        <SelectValue placeholder="Todos los roles" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">Todos los roles</SelectItem>
                        {availableRoleOptions.map((role) => (
                          <SelectItem key={role.value} value={role.value}>
                            {role.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="table-wrapper">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Usuario</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Puesto</TableHead>
                        <TableHead>Rol</TableHead>
                        <TableHead className="w-56 text-right">Acciones</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {loading ? (
                        [...Array(3)].map((_, i) => (
                          <TableRow key={i}>
                            {[...Array(5)].map((_, j) => (
                              <TableCell key={j}><Skeleton className="h-5 w-full" /></TableCell>
                            ))}
                          </TableRow>
                        ))
                      ) : filteredUsers.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={5} className="text-center py-12 text-muted-foreground">
                            No hay usuarios con ese filtro
                          </TableCell>
                        </TableRow>
                      ) : (
                        filteredUsers.map((u) => (
                          <TableRow key={u.id} data-testid={`user-row-${u.id}`}>
                            <TableCell className="font-medium">{u.name}</TableCell>
                            <TableCell>{u.email}</TableCell>
                            <TableCell>{u.position || '-'}</TableCell>
                            <TableCell>
                              <Select
                                value={u.role}
                                onValueChange={(value) => handleUpdateUserRole(u.id, value)}
                                disabled={u.id === user?.id}
                              >
                                <SelectTrigger className="w-[150px]" data-testid={`user-role-select-${u.id}`}>
                                  <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                  {availableRoleOptions.map((role) => (
                                    <SelectItem key={role.value} value={role.value}>
                                      {role.label}
                                    </SelectItem>
                                  ))}
                                </SelectContent>
                              </Select>
                            </TableCell>
                            <TableCell>
                              <div className="flex items-center justify-end gap-2">
                                {u.id === user?.id ? (
                                  <Badge variant="outline" className="gap-1">
                                    <Shield size={12} /> Tú
                                  </Badge>
                                ) : (
                                  <>
                                    <Button
                                      type="button"
                                      size="sm"
                                      variant="outline"
                                      onClick={() => openEditUserDialog(u)}
                                      data-testid={`edit-user-btn-${u.id}`}
                                    >
                                      <Pencil size={14} className="mr-1" /> Editar
                                    </Button>
                                    <Button
                                      type="button"
                                      size="sm"
                                      variant="destructive"
                                      onClick={() => handleDeleteUser(u)}
                                      data-testid={`delete-user-btn-${u.id}`}
                                    >
                                      <Trash size={14} className="mr-1" /> Borrar
                                    </Button>
                                  </>
                                )}
                              </div>
                            </TableCell>
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        )}
      </Tabs>
    </div>
  );
}
