import { useState, useEffect, useCallback } from 'react';
import { vehiclesApi, agenciesApi } from '../lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Skeleton } from '../components/ui/skeleton';
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '../components/ui/dropdown-menu';
import { Plus, Upload, MagnifyingGlass, Funnel, DotsThree, Car } from '@phosphor-icons/react';
import { toast } from 'sonner';

function AgingBadge({ days }) {
  if (days <= 30) return <Badge className="aging-low">{days} días</Badge>;
  if (days <= 60) return <Badge className="aging-medium">{days} días</Badge>;
  return <Badge className="aging-high">{days} días</Badge>;
}

function StatusBadge({ status }) {
  const statusMap = {
    in_stock: { label: 'En Stock', className: 'status-in_stock' },
    sold: { label: 'Vendido', className: 'status-sold' },
    transferred: { label: 'Transferido', className: 'status-transferred' }
  };
  const config = statusMap[status] || statusMap.in_stock;
  return <Badge className={config.className}>{config.label}</Badge>;
}

export default function InventoryPage() {
  const [vehicles, setVehicles] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterAgency, setFilterAgency] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const [filterType, setFilterType] = useState('');
  const [isAddDialogOpen, setIsAddDialogOpen] = useState(false);
  const [isImportDialogOpen, setIsImportDialogOpen] = useState(false);
  const [newVehicle, setNewVehicle] = useState({
    vin: '',
    model: '',
    year: new Date().getFullYear(),
    trim: '',
    color: '',
    vehicle_type: 'new',
    purchase_price: '',
    agency_id: ''
  });

  const fetchData = useCallback(async () => {
    try {
      const [vehiclesRes, agenciesRes] = await Promise.all([
        vehiclesApi.getAll({
          agency_id: filterAgency || undefined,
          status: filterStatus || undefined,
          vehicle_type: filterType || undefined
        }),
        agenciesApi.getAll()
      ]);
      setVehicles(vehiclesRes.data);
      setAgencies(agenciesRes.data);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [filterAgency, filterStatus, filterType]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleAddVehicle = async (e) => {
    e.preventDefault();
    try {
      await vehiclesApi.create({
        ...newVehicle,
        purchase_price: parseFloat(newVehicle.purchase_price),
        year: parseInt(newVehicle.year)
      });
      toast.success('Vehículo agregado correctamente');
      setIsAddDialogOpen(false);
      setNewVehicle({
        vin: '',
        model: '',
        year: new Date().getFullYear(),
        trim: '',
        color: '',
        vehicle_type: 'new',
        purchase_price: '',
        agency_id: ''
      });
      fetchData();
    } catch (error) {
      toast.error('Error al agregar vehículo');
    }
  };

  const handleImport = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    try {
      const result = await vehiclesApi.import(file);
      toast.success(`${result.data.imported} vehículos importados correctamente`);
      if (result.data.errors.length > 0) {
        toast.warning(`${result.data.errors.length} errores encontrados`);
      }
      setIsImportDialogOpen(false);
      fetchData();
    } catch (error) {
      toast.error('Error al importar archivo');
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0
    }).format(value);
  };

  const filteredVehicles = vehicles.filter((v) => {
    if (searchTerm) {
      const search = searchTerm.toLowerCase();
      return (
        v.vin?.toLowerCase().includes(search) ||
        v.model?.toLowerCase().includes(search) ||
        v.trim?.toLowerCase().includes(search) ||
        v.color?.toLowerCase().includes(search)
      );
    }
    return true;
  });

  return (
    <div className="space-y-6" data-testid="inventory-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Inventario de Vehículos
          </h1>
          <p className="text-muted-foreground">
            {filteredVehicles.length} vehículos encontrados
          </p>
        </div>
        <div className="flex gap-2">
          <Dialog open={isImportDialogOpen} onOpenChange={setIsImportDialogOpen}>
            <DialogTrigger asChild>
              <Button variant="outline" data-testid="import-vehicles-btn">
                <Upload size={18} className="mr-2" />
                Importar
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Importar Vehículos</DialogTitle>
              </DialogHeader>
              <div className="space-y-4">
                <p className="text-sm text-muted-foreground">
                  Sube un archivo CSV o Excel con las columnas: vin, model, year, trim, color, vehicle_type, purchase_price, agency_id
                </p>
                <Input
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  onChange={handleImport}
                  data-testid="import-file-input"
                />
              </div>
            </DialogContent>
          </Dialog>

          <Dialog open={isAddDialogOpen} onOpenChange={setIsAddDialogOpen}>
            <DialogTrigger asChild>
              <Button className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="add-vehicle-btn">
                <Plus size={18} className="mr-2" />
                Agregar Vehículo
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-[500px]">
              <DialogHeader>
                <DialogTitle>Agregar Vehículo</DialogTitle>
              </DialogHeader>
              <form onSubmit={handleAddVehicle} className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="vin">VIN</Label>
                    <Input
                      id="vin"
                      value={newVehicle.vin}
                      onChange={(e) => setNewVehicle({ ...newVehicle, vin: e.target.value })}
                      required
                      data-testid="vehicle-vin-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="model">Modelo</Label>
                    <Input
                      id="model"
                      value={newVehicle.model}
                      onChange={(e) => setNewVehicle({ ...newVehicle, model: e.target.value })}
                      required
                      data-testid="vehicle-model-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="year">Año</Label>
                    <Input
                      id="year"
                      type="number"
                      value={newVehicle.year}
                      onChange={(e) => setNewVehicle({ ...newVehicle, year: e.target.value })}
                      required
                      data-testid="vehicle-year-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="trim">Trim</Label>
                    <Input
                      id="trim"
                      value={newVehicle.trim}
                      onChange={(e) => setNewVehicle({ ...newVehicle, trim: e.target.value })}
                      required
                      data-testid="vehicle-trim-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="color">Color</Label>
                    <Input
                      id="color"
                      value={newVehicle.color}
                      onChange={(e) => setNewVehicle({ ...newVehicle, color: e.target.value })}
                      required
                      data-testid="vehicle-color-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="vehicle_type">Tipo</Label>
                    <Select
                      value={newVehicle.vehicle_type}
                      onValueChange={(value) => setNewVehicle({ ...newVehicle, vehicle_type: value })}
                    >
                      <SelectTrigger data-testid="vehicle-type-select">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="new">Nuevo</SelectItem>
                        <SelectItem value="used">Seminuevo</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="purchase_price">Precio de Compra</Label>
                    <Input
                      id="purchase_price"
                      type="number"
                      value={newVehicle.purchase_price}
                      onChange={(e) => setNewVehicle({ ...newVehicle, purchase_price: e.target.value })}
                      required
                      data-testid="vehicle-price-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="agency_id">Agencia</Label>
                    <Select
                      value={newVehicle.agency_id}
                      onValueChange={(value) => setNewVehicle({ ...newVehicle, agency_id: value })}
                    >
                      <SelectTrigger data-testid="vehicle-agency-select">
                        <SelectValue placeholder="Seleccionar" />
                      </SelectTrigger>
                      <SelectContent>
                        {agencies.map((agency) => (
                          <SelectItem key={agency.id} value={agency.id}>
                            {agency.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="flex justify-end gap-2">
                  <Button type="button" variant="outline" onClick={() => setIsAddDialogOpen(false)}>
                    Cancelar
                  </Button>
                  <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-vehicle-btn">
                    Guardar
                  </Button>
                </div>
              </form>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {/* Filters */}
      <Card className="border-border/40">
        <CardContent className="p-4">
          <div className="flex flex-col sm:flex-row gap-4">
            <div className="flex-1 relative">
              <MagnifyingGlass size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Buscar por VIN, modelo, trim o color..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10"
                data-testid="search-vehicles-input"
              />
            </div>
            <Select value={filterAgency} onValueChange={setFilterAgency}>
              <SelectTrigger className="w-full sm:w-[180px]" data-testid="filter-agency-select">
                <SelectValue placeholder="Todas las agencias" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="">Todas las agencias</SelectItem>
                {agencies.map((agency) => (
                  <SelectItem key={agency.id} value={agency.id}>
                    {agency.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={filterStatus} onValueChange={setFilterStatus}>
              <SelectTrigger className="w-full sm:w-[150px]" data-testid="filter-status-select">
                <SelectValue placeholder="Todos los estados" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="">Todos los estados</SelectItem>
                <SelectItem value="in_stock">En Stock</SelectItem>
                <SelectItem value="sold">Vendido</SelectItem>
                <SelectItem value="transferred">Transferido</SelectItem>
              </SelectContent>
            </Select>
            <Select value={filterType} onValueChange={setFilterType}>
              <SelectTrigger className="w-full sm:w-[150px]" data-testid="filter-type-select">
                <SelectValue placeholder="Todos los tipos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="">Todos los tipos</SelectItem>
                <SelectItem value="new">Nuevos</SelectItem>
                <SelectItem value="used">Seminuevos</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Table */}
      <Card className="border-border/40">
        <div className="table-wrapper">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Vehículo</TableHead>
                <TableHead>VIN</TableHead>
                <TableHead>Agencia</TableHead>
                <TableHead>Tipo</TableHead>
                <TableHead className="text-right">Precio</TableHead>
                <TableHead>Aging</TableHead>
                <TableHead className="text-right">Costo Fin.</TableHead>
                <TableHead>Estado</TableHead>
                <TableHead className="w-12"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                [...Array(5)].map((_, i) => (
                  <TableRow key={i}>
                    {[...Array(9)].map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton className="h-5 w-full" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : filteredVehicles.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="text-center py-12">
                    <Car size={48} className="mx-auto text-muted-foreground mb-4" />
                    <p className="text-muted-foreground">No se encontraron vehículos</p>
                  </TableCell>
                </TableRow>
              ) : (
                filteredVehicles.map((vehicle) => (
                  <TableRow key={vehicle.id} data-testid={`vehicle-row-${vehicle.id}`}>
                    <TableCell>
                      <div className="font-medium">{vehicle.model} {vehicle.trim}</div>
                      <div className="text-sm text-muted-foreground">{vehicle.year} • {vehicle.color}</div>
                    </TableCell>
                    <TableCell className="font-mono text-sm">{vehicle.vin}</TableCell>
                    <TableCell>
                      <div>{vehicle.agency_name}</div>
                      <div className="text-sm text-muted-foreground">{vehicle.brand_name}</div>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">
                        {vehicle.vehicle_type === 'new' ? 'Nuevo' : 'Seminuevo'}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatCurrency(vehicle.purchase_price)}
                    </TableCell>
                    <TableCell>
                      <AgingBadge days={vehicle.aging_days} />
                    </TableCell>
                    <TableCell className="text-right tabular-nums text-[#E63946]">
                      {formatCurrency(vehicle.financial_cost)}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={vehicle.status} />
                    </TableCell>
                    <TableCell>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button variant="ghost" size="sm">
                            <DotsThree size={20} />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem>Ver detalles</DropdownMenuItem>
                          <DropdownMenuItem>Editar</DropdownMenuItem>
                          <DropdownMenuItem>Registrar venta</DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}
