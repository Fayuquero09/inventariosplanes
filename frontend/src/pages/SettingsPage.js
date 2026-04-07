import { useState, useEffect, useCallback } from 'react';
import { groupsApi, brandsApi, agenciesApi, usersApi } from '../lib/api';
import { useAuth } from '../contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
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
  Storefront,
  Users,
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

export default function SettingsPage() {
  const { user, isAdmin } = useAuth();
  const [groups, setGroups] = useState([]);
  const [brands, setBrands] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  
  // Dialog states
  const [groupDialog, setGroupDialog] = useState(false);
  const [brandDialog, setBrandDialog] = useState(false);
  const [agencyDialog, setAgencyDialog] = useState(false);
  
  // Form states
  const [groupForm, setGroupForm] = useState({ name: '', description: '' });
  const [brandForm, setBrandForm] = useState({ name: '', group_id: '', logo_url: '' });
  const [agencyForm, setAgencyForm] = useState({ name: '', brand_id: '', address: '', city: '' });

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

  const handleCreateGroup = async (e) => {
    e.preventDefault();
    try {
      await groupsApi.create(groupForm);
      toast.success('Grupo creado correctamente');
      setGroupDialog(false);
      setGroupForm({ name: '', description: '' });
      fetchData();
    } catch (error) {
      toast.error('Error al crear grupo');
    }
  };

  const handleCreateBrand = async (e) => {
    e.preventDefault();
    try {
      await brandsApi.create(brandForm);
      toast.success('Marca creada correctamente');
      setBrandDialog(false);
      setBrandForm({ name: '', group_id: '', logo_url: '' });
      fetchData();
    } catch (error) {
      toast.error('Error al crear marca');
    }
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

  const handleUpdateUserRole = async (userId, newRole) => {
    try {
      await usersApi.update(userId, { role: newRole });
      toast.success('Rol actualizado');
      fetchData();
    } catch (error) {
      toast.error('Error al actualizar rol');
    }
  };

  const getGroupName = (id) => groups.find((g) => g.id === id)?.name || '-';
  const getBrandName = (id) => brands.find((b) => b.id === id)?.name || '-';
  const getRoleLabel = (role) => ROLES.find((r) => r.value === role)?.label || role;

  return (
    <div className="space-y-6" data-testid="settings-page">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
          Configuración
        </h1>
        <p className="text-muted-foreground">
          Gestiona grupos, marcas, agencias y usuarios
        </p>
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
                  <CardDescription>Distribuidores o grupos empresariales</CardDescription>
                </div>
              </div>
              {isAdmin && (
                <Dialog open={groupDialog} onOpenChange={setGroupDialog}>
                  <DialogTrigger asChild>
                    <Button variant="outline" size="sm" data-testid="add-group-btn">
                      <Plus size={16} className="mr-1" /> Agregar
                    </Button>
                  </DialogTrigger>
                  <DialogContent>
                    <DialogHeader>
                      <DialogTitle>Nuevo Grupo</DialogTitle>
                    </DialogHeader>
                    <form onSubmit={handleCreateGroup} className="space-y-4">
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
                      <div className="flex justify-end gap-2">
                        <Button type="button" variant="outline" onClick={() => setGroupDialog(false)}>Cancelar</Button>
                        <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-group-btn">Crear</Button>
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
                <div className="space-y-2">
                  {groups.map((group) => (
                    <div key={group.id} className="flex items-center justify-between p-3 rounded-md bg-muted/30" data-testid={`group-${group.id}`}>
                      <div>
                        <div className="font-medium">{group.name}</div>
                        {group.description && <div className="text-sm text-muted-foreground">{group.description}</div>}
                      </div>
                      <Badge variant="outline">{brands.filter((b) => b.group_id === group.id).length} marcas</Badge>
                    </div>
                  ))}
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
                  <CardDescription>Marcas de vehículos por grupo</CardDescription>
                </div>
              </div>
              <Dialog open={brandDialog} onOpenChange={setBrandDialog}>
                <DialogTrigger asChild>
                  <Button variant="outline" size="sm" data-testid="add-brand-btn">
                    <Plus size={16} className="mr-1" /> Agregar
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Nueva Marca</DialogTitle>
                  </DialogHeader>
                  <form onSubmit={handleCreateBrand} className="space-y-4">
                    <div className="space-y-2">
                      <Label>Grupo</Label>
                      <Select value={brandForm.group_id} onValueChange={(v) => setBrandForm({ ...brandForm, group_id: v })} required>
                        <SelectTrigger data-testid="brand-group-select">
                          <SelectValue placeholder="Seleccionar grupo" />
                        </SelectTrigger>
                        <SelectContent>
                          {groups.map((g) => (
                            <SelectItem key={g.id} value={g.id}>{g.name}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Nombre</Label>
                      <Input
                        value={brandForm.name}
                        onChange={(e) => setBrandForm({ ...brandForm, name: e.target.value })}
                        required
                        data-testid="brand-name-input"
                      />
                    </div>
                    <div className="flex justify-end gap-2">
                      <Button type="button" variant="outline" onClick={() => setBrandDialog(false)}>Cancelar</Button>
                      <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-brand-btn">Crear</Button>
                    </div>
                  </form>
                </DialogContent>
              </Dialog>
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
                    <div key={brand.id} className="flex items-center justify-between p-3 rounded-md bg-muted/30" data-testid={`brand-${brand.id}`}>
                      <div>
                        <div className="font-medium">{brand.name}</div>
                        <div className="text-sm text-muted-foreground">{getGroupName(brand.group_id)}</div>
                      </div>
                      <Badge variant="outline">{agencies.filter((a) => a.brand_id === brand.id).length} agencias</Badge>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Agencies */}
          <Card className="border-border/40">
            <CardHeader className="flex flex-row items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-md bg-[#E9C46A]/10 flex items-center justify-center">
                  <Storefront size={20} weight="duotone" className="text-[#b89830]" />
                </div>
                <div>
                  <CardTitle className="text-lg">Agencias</CardTitle>
                  <CardDescription>Puntos de venta por marca</CardDescription>
                </div>
              </div>
              <Dialog open={agencyDialog} onOpenChange={setAgencyDialog}>
                <DialogTrigger asChild>
                  <Button variant="outline" size="sm" data-testid="add-agency-btn">
                    <Plus size={16} className="mr-1" /> Agregar
                  </Button>
                </DialogTrigger>
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
            </CardHeader>
            <CardContent>
              {loading ? (
                <div className="space-y-2">
                  {[1, 2, 3].map((i) => <Skeleton key={i} className="h-12 w-full" />)}
                </div>
              ) : agencies.length === 0 ? (
                <p className="text-center py-8 text-muted-foreground">No hay agencias configuradas</p>
              ) : (
                <div className="space-y-2">
                  {agencies.map((agency) => (
                    <div key={agency.id} className="flex items-center justify-between p-3 rounded-md bg-muted/30" data-testid={`agency-${agency.id}`}>
                      <div>
                        <div className="font-medium">{agency.name}</div>
                        <div className="text-sm text-muted-foreground">
                          {agency.brand_name || getBrandName(agency.brand_id)} • {agency.city || 'Sin ciudad'}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
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
