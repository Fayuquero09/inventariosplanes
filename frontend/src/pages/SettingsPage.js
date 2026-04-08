import { useState, useEffect, useCallback } from 'react';
import { groupsApi, brandsApi, agenciesApi, usersApi, organizationImportApi, authApi, sellersApi, vehicleCatalogApi } from '../lib/api';
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
  Shield
} from '@phosphor-icons/react';
import { toast } from 'sonner';

const ROLES = [
  { value: 'app_admin', label: 'Admin App' },
  { value: 'app_user', label: 'Usuario App' },
  { value: 'group_admin', label: 'Admin Grupo' },
  { value: 'brand_admin', label: 'Admin Marca' },
  { value: 'agency_admin', label: 'Admin Agencia' },
  { value: 'group_user', label: 'Usuario Grupo' },
  { value: 'brand_user', label: 'Usuario Marca' },
  { value: 'agency_user', label: 'Usuario Agencia' },
  { value: 'seller', label: 'Vendedor' }
];

const normalizeBrandKey = (value) => (value || '').trim().toLowerCase();

export default function SettingsPage() {
  const { user, isAdmin } = useAuth();
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
  const [sellerDialog, setSellerDialog] = useState(false);
  const [importDialog, setImportDialog] = useState(false);
  const [editingGroup, setEditingGroup] = useState(null);
  const [groupBrandsToAssign, setGroupBrandsToAssign] = useState([]);
  const [importingStructure, setImportingStructure] = useState(false);
  const [deletingGroup, setDeletingGroup] = useState(false);
  const [groupDeleteError, setGroupDeleteError] = useState('');
  const [showCascadeDelete, setShowCascadeDelete] = useState(false);
  
  // Form states
  const [groupForm, setGroupForm] = useState({ name: '', description: '' });
  const [agencyForm, setAgencyForm] = useState({ name: '', brand_id: '', address: '', city: '' });
  const [sellerForm, setSellerForm] = useState({ name: '', email: '', password: '', agency_id: '' });

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

      if (isAdmin) {
        const usersRes = await usersApi.getAll();
        setUsers(usersRes.data);
      }
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [isAdmin]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    fetchCatalogMakes();
  }, [fetchCatalogMakes]);

  const handleSaveGroup = async (e) => {
    e.preventDefault();
    try {
      let targetGroupId = editingGroup?.id || null;

      if (editingGroup?.id) {
        await groupsApi.update(editingGroup.id, groupForm);
      } else {
        const created = await groupsApi.create(groupForm);
        targetGroupId = created?.data?.id || null;
      }

      if (targetGroupId && groupBrandsToAssign.length > 0) {
        const selectedMakes = groupBrandsToAssign
          .map((name) => (name || '').trim())
          .filter(Boolean);

        await Promise.all(
          selectedMakes.map(async (makeName) => {
            const existing = brands.find((brand) => normalizeBrandKey(brand.name) === normalizeBrandKey(makeName));
            if (existing) {
              return brandsApi.update(existing.id, {
                name: existing.name,
                group_id: targetGroupId,
                logo_url: existing.logo_url || ''
              });
            }
            return brandsApi.create({
              name: makeName,
              group_id: targetGroupId,
              logo_url: ''
            });
          })
        );
      }

      const assignedMessage = groupBrandsToAssign.length > 0
        ? ` y ${groupBrandsToAssign.length} marca(s) reasignada(s)`
        : '';
      toast.success(`${editingGroup ? 'Grupo actualizado' : 'Grupo creado'} correctamente${assignedMessage}`);

      setGroupDialog(false);
      setEditingGroup(null);
      setGroupBrandsToAssign([]);
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

  const handleCreateAgency = async (e) => {
    e.preventDefault();
    try {
      await agenciesApi.create(agencyForm);
      toast.success('Agencia creada correctamente');
      setAgencyDialog(false);
      setAgencyForm({ name: '', brand_id: '', address: '', city: '' });
      fetchData();
    } catch (error) {
      toast.error('Error al crear agencia');
    }
  };

  const openAgencyDialogForBrand = (brandId) => {
    setAgencyForm((prev) => ({ ...prev, brand_id: brandId }));
    setAgencyDialog(true);
  };

  const handleCreateSeller = async (e) => {
    e.preventDefault();
    try {
      const agency = agencies.find((a) => a.id === sellerForm.agency_id);
      if (!agency) {
        toast.error('Selecciona una agencia valida');
        return;
      }

      await authApi.register({
        name: sellerForm.name,
        email: (sellerForm.email || '').trim(),
        password: sellerForm.password,
        role: 'seller',
        group_id: agency.group_id,
        brand_id: agency.brand_id,
        agency_id: agency.id
      });

      toast.success('Vendedor creado correctamente');
      setSellerDialog(false);
      setSellerForm({ name: '', email: '', password: '', agency_id: '' });
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al crear vendedor');
    }
  };

  const openSellerDialogForAgency = (agencyId) => {
    setSellerForm((prev) => ({ ...prev, agency_id: agencyId }));
    setSellerDialog(true);
  };

  const handleUpdateUserRole = async (userId, newRole) => {
    try {
      await usersApi.update(userId, { role: newRole });
      toast.success('Rol actualizado');
      fetchData();
    } catch (error) {
      toast.error('Error al actualizar rol');
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
    if (key && !brandsByNormalizedName.has(key)) {
      brandsByNormalizedName.set(key, brand);
    }
  });

  const catalogBrandRows = (catalogMakes || []).map((make) => {
    const name = (make?.name || '').trim();
    const existing = brandsByNormalizedName.get(normalizeBrandKey(name));
    return {
      key: normalizeBrandKey(name),
      name,
      group_id: existing?.group_id || null,
      id: existing?.id || null
    };
  }).filter((row) => row.name);

  const sourceBrandRows = catalogBrandRows.length > 0
    ? catalogBrandRows
    : brands.map((brand) => ({
      key: normalizeBrandKey(brand.name),
      name: brand.name,
      group_id: brand.group_id,
      id: brand.id
    }));

  const currentGroupBrands = editingGroup ? sourceBrandRows.filter((b) => b.group_id === editingGroup.id) : [];
  const reassignableBrands = editingGroup ? sourceBrandRows.filter((b) => b.group_id !== editingGroup.id) : [];
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

  const sellersByAgencyId = sellers.reduce((acc, seller) => {
    const key = seller.agency_id || '__none__';
    if (!acc[key]) acc[key] = [];
    acc[key].push(seller);
    return acc;
  }, {});

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

      <Tabs defaultValue="structure" className="w-full">
        <TabsList>
          <TabsTrigger value="structure" data-testid="tab-structure">Estructura</TabsTrigger>
          {isAdmin && <TabsTrigger value="users" data-testid="tab-users">Usuarios</TabsTrigger>}
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
                      {editingGroup && canManageGroupStructure && (
                        <div className="space-y-3 rounded-md border border-border/50 p-3">
                          <div>
                            <Label className="text-sm">Marcas actualmente en este grupo</Label>
                            {currentGroupBrands.length === 0 ? (
                              <p className="text-xs text-muted-foreground mt-1">Este grupo aún no tiene marcas asignadas.</p>
                            ) : (
                              <div className="flex flex-wrap gap-2 mt-2">
                                {currentGroupBrands.map((brand) => (
                                  <Badge key={brand.key} variant="secondary">
                                    {brand.name}
                                  </Badge>
                                ))}
                              </div>
                            )}
                          </div>

                          <div className="space-y-2">
                            <Label className="text-sm">Agregar marcas del catálogo Strapi a este grupo</Label>
                            {loadingCatalog ? (
                              <p className="text-xs text-muted-foreground">Cargando catálogo de marcas...</p>
                            ) : reassignableBrands.length === 0 ? (
                              <p className="text-xs text-muted-foreground">No hay marcas del catálogo disponibles para reasignar.</p>
                            ) : (
                              <div className="max-h-40 overflow-y-auto space-y-2 pr-1">
                                {reassignableBrands.map((brand) => (
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
                                      Grupo actual: {brand.group_id ? getGroupName(brand.group_id) : 'Sin asignar'}
                                    </span>
                                  </label>
                                ))}
                              </div>
                            )}
                            <p className="text-xs text-muted-foreground">
                              Las marcas seleccionadas se moverán a este grupo al guardar.
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
                <div className="space-y-3">
                  {groups.map((group) => {
                    const groupBrands = brandsByGroupId[group.id] || [];
                    const groupAgencyCount = groupBrands.reduce(
                      (total, brand) => total + ((agenciesByBrandId[brand.id] || []).length),
                      0
                    );
                    const groupSellerCount = groupBrands.reduce(
                      (total, brand) => total + (agenciesByBrandId[brand.id] || []).reduce(
                        (agencyTotal, agency) => agencyTotal + ((sellersByAgencyId[agency.id] || []).length),
                        0
                      ),
                      0
                    );
                    const canManageThisGroup = canManageGroupStructure && (isAdmin || !user?.group_id || user.group_id === group.id);

                    return (
                      <div key={group.id} className="rounded-md border border-border/60" data-testid={`group-${group.id}`}>
                        <div className="flex items-start justify-between gap-4 p-3 bg-muted/20">
                          <div>
                            <div className="font-medium">{group.name}</div>
                            {group.description && <div className="text-sm text-muted-foreground">{group.description}</div>}
                          </div>
                          <div className="flex items-center gap-2">
                            <Badge variant="outline">{groupBrands.length} marcas</Badge>
                            <Badge variant="outline">{groupAgencyCount} agencias</Badge>
                            <Badge variant="outline">{groupSellerCount} vendedores</Badge>
                            {canManageThisGroup && (
                              <Button
                                type="button"
                                variant="ghost"
                                size="sm"
                                onClick={() => openEditGroup(group)}
                                data-testid={`edit-group-btn-${group.id}`}
                              >
                                <Pencil size={16} />
                              </Button>
                            )}
                          </div>
                        </div>

                        <div className="space-y-3 p-3">
                          {groupBrands.length === 0 ? (
                            <p className="text-sm text-muted-foreground">
                              Sin marcas asignadas. Usa editar grupo para asignar marcas del catálogo.
                            </p>
                          ) : (
                            groupBrands.map((brand) => {
                              const brandAgencies = agenciesByBrandId[brand.id] || [];
                              return (
                                <div key={brand.id} className="rounded-md border border-border/50 p-3" data-testid={`brand-${brand.id}`}>
                                  <div className="flex items-center justify-between gap-2">
                                    <div className="font-medium">{brand.name}</div>
                                    <div className="flex items-center gap-2">
                                      <Badge variant="outline">{brandAgencies.length} agencias</Badge>
                                      {canManageThisGroup && (
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
                                  </div>

                                  <div className="mt-2 space-y-2">
                                    {brandAgencies.length === 0 ? (
                                      <p className="text-sm text-muted-foreground">Sin agencias registradas.</p>
                                    ) : (
                                      brandAgencies.map((agency) => {
                                        const agencySellers = sellersByAgencyId[agency.id] || [];
                                        return (
                                          <div key={agency.id} className="rounded-md bg-muted/30 p-3" data-testid={`agency-${agency.id}`}>
                                            <div className="flex items-start justify-between gap-3">
                                              <div>
                                                <div className="font-medium">{agency.name}</div>
                                                <div className="text-sm text-muted-foreground">
                                                  {agency.city || 'Sin ciudad'}
                                                  {agency.address ? ` • ${agency.address}` : ''}
                                                </div>
                                              </div>
                                              {canManageThisGroup && (
                                                <Button
                                                  type="button"
                                                  size="sm"
                                                  variant="ghost"
                                                  onClick={() => openSellerDialogForAgency(agency.id)}
                                                  data-testid={`add-seller-for-agency-${agency.id}`}
                                                >
                                                  <UserPlus size={14} className="mr-1" /> Vendedor
                                                </Button>
                                              )}
                                            </div>
                                            <div className="mt-2 flex flex-wrap items-center gap-2">
                                              {agencySellers.length === 0 ? (
                                                <span className="text-xs text-muted-foreground">Sin vendedores</span>
                                              ) : (
                                                agencySellers.map((seller) => (
                                                  <Badge key={seller.id} variant="secondary">{seller.name}</Badge>
                                                ))
                                              )}
                                            </div>
                                          </div>
                                        );
                                      })
                                    )}
                                  </div>
                                </div>
                              );
                            })
                          )}
                        </div>
                      </div>
                    );
                  })}
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
                  <CardDescription>Vista informativa. La edición se realiza en Grupos.</CardDescription>
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
              ) : brands.length === 0 ? (
                <p className="text-center py-8 text-muted-foreground">No hay marcas configuradas</p>
              ) : (
                <div className="space-y-2">
                  {brands.map((brand) => (
                    <div key={brand.id} className="flex items-center justify-between p-3 rounded-md bg-muted/30" data-testid={`brand-summary-${brand.id}`}>
                      <div>
                        <div className="font-medium">{brand.name}</div>
                        <div className="text-sm text-muted-foreground">{getGroupName(brand.group_id)}</div>
                      </div>
                      <Badge variant="outline">{(agenciesByBrandId[brand.id] || []).length} agencias</Badge>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {canManageGroupStructure && (
            <>
              <Dialog open={agencyDialog} onOpenChange={setAgencyDialog}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Nueva Agencia</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleCreateAgency} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Marca</Label>
                      <Select value={agencyForm.brand_id} onValueChange={(v) => setAgencyForm({ ...agencyForm, brand_id: v })} required>
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
                    <div className="space-y-2">
                      <Label>Ciudad</Label>
                      <Input
                        value={agencyForm.city}
                        onChange={(e) => setAgencyForm({ ...agencyForm, city: e.target.value })}
                        data-testid="agency-city-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Dirección</Label>
                      <Input
                        value={agencyForm.address}
                        onChange={(e) => setAgencyForm({ ...agencyForm, address: e.target.value })}
                        data-testid="agency-address-input"
                      />
                    </div>
                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => setAgencyDialog(false)}>Cancelar</Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-agency-btn">Crear</Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>

              <Dialog open={sellerDialog} onOpenChange={setSellerDialog}>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Nuevo Vendedor</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleCreateSeller} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Agencia</Label>
                      <Select value={sellerForm.agency_id} onValueChange={(v) => setSellerForm({ ...sellerForm, agency_id: v })} required>
                        <SelectTrigger data-testid="seller-agency-select">
                          <SelectValue placeholder="Seleccionar agencia" />
                        </SelectTrigger>
                        <SelectContent>
                          {agencies.map((a) => (
                            <SelectItem key={a.id} value={a.id}>{a.name}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Nombre</Label>
                      <Input
                        value={sellerForm.name}
                        onChange={(e) => setSellerForm({ ...sellerForm, name: e.target.value })}
                        required
                        data-testid="seller-name-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Email</Label>
                      <Input
                        type="email"
                        value={sellerForm.email}
                        onChange={(e) => setSellerForm({ ...sellerForm, email: e.target.value })}
                        required
                        data-testid="seller-email-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Contraseña temporal</Label>
                      <Input
                        type="password"
                        value={sellerForm.password}
                        onChange={(e) => setSellerForm({ ...sellerForm, password: e.target.value })}
                        required
                        data-testid="seller-password-input"
                      />
                    </div>
                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => setSellerDialog(false)}>Cancelar</Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-seller-btn">Crear</Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>
            </>
          )}
        </TabsContent>

        {isAdmin && (
          <TabsContent value="users" className="mt-6">
            <Card className="border-border/40">
              <CardHeader>
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-md bg-[#002FA7]/10 flex items-center justify-center">
                    <Users size={20} weight="duotone" className="text-[#002FA7]" />
                  </div>
                  <div>
                    <CardTitle className="text-lg">Usuarios</CardTitle>
                    <CardDescription>Gestiona roles y permisos de usuarios</CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="table-wrapper">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Usuario</TableHead>
                        <TableHead>Email</TableHead>
                        <TableHead>Rol</TableHead>
                        <TableHead className="w-24"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {loading ? (
                        [...Array(3)].map((_, i) => (
                          <TableRow key={i}>
                            {[...Array(4)].map((_, j) => (
                              <TableCell key={j}><Skeleton className="h-5 w-full" /></TableCell>
                            ))}
                          </TableRow>
                        ))
                      ) : users.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={4} className="text-center py-12 text-muted-foreground">
                            No hay usuarios
                          </TableCell>
                        </TableRow>
                      ) : (
                        users.map((u) => (
                          <TableRow key={u.id} data-testid={`user-row-${u.id}`}>
                            <TableCell className="font-medium">{u.name}</TableCell>
                            <TableCell>{u.email}</TableCell>
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
                                  {ROLES.map((role) => (
                                    <SelectItem key={role.value} value={role.value}>
                                      {role.label}
                                    </SelectItem>
                                  ))}
                                </SelectContent>
                              </Select>
                            </TableCell>
                            <TableCell>
                              {u.id === user?.id && (
                                <Badge variant="outline" className="gap-1">
                                  <Shield size={12} /> Tú
                                </Badge>
                              )}
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
