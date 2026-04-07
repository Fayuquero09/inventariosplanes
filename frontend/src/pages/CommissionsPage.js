import { useState, useEffect, useCallback } from 'react';
import { commissionRulesApi, agenciesApi } from '../lib/api';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../components/ui/card';
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
import { Plus, Pencil, Trash, CurrencyDollar, Percent, Package, Coin } from '@phosphor-icons/react';
import { toast } from 'sonner';

const RULE_TYPES = [
  { value: 'per_unit', label: 'Por Unidad', icon: Package, description: 'Monto fijo por cada unidad vendida' },
  { value: 'percentage', label: 'Porcentaje', icon: Percent, description: 'Porcentaje del precio de venta' },
  { value: 'volume_bonus', label: 'Bono por Volumen', icon: CurrencyDollar, description: 'Bono al alcanzar cierto número de unidades' },
  { value: 'fi_bonus', label: 'Bono F&I', icon: Coin, description: 'Porcentaje sobre ingresos de F&I' }
];

export default function CommissionsPage() {
  const [rules, setRules] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [selectedAgency, setSelectedAgency] = useState('all');
  const [loading, setLoading] = useState(true);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [formData, setFormData] = useState({
    agency_id: '',
    name: '',
    rule_type: 'per_unit',
    value: '',
    min_units: '',
    max_units: ''
  });

  const fetchData = useCallback(async () => {
    try {
      const [rulesRes, agenciesRes] = await Promise.all([
        commissionRulesApi.getAll(selectedAgency !== 'all' ? selectedAgency : undefined),
        agenciesApi.getAll()
      ]);
      setRules(rulesRes.data);
      setAgencies(agenciesRes.data);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [selectedAgency]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const data = {
        ...formData,
        value: parseFloat(formData.value),
        min_units: formData.min_units ? parseInt(formData.min_units) : null,
        max_units: formData.max_units ? parseInt(formData.max_units) : null
      };

      if (editingRule) {
        await commissionRulesApi.update(editingRule.id, data);
        toast.success('Regla actualizada correctamente');
      } else {
        await commissionRulesApi.create(data);
        toast.success('Regla creada correctamente');
      }

      setIsDialogOpen(false);
      setEditingRule(null);
      setFormData({
        agency_id: '',
        name: '',
        rule_type: 'per_unit',
        value: '',
        min_units: '',
        max_units: ''
      });
      fetchData();
    } catch (error) {
      toast.error('Error al guardar regla');
    }
  };

  const handleEdit = (rule) => {
    setEditingRule(rule);
    setFormData({
      agency_id: rule.agency_id,
      name: rule.name,
      rule_type: rule.rule_type,
      value: rule.value.toString(),
      min_units: rule.min_units?.toString() || '',
      max_units: rule.max_units?.toString() || ''
    });
    setIsDialogOpen(true);
  };

  const handleDelete = async (id) => {
    if (!window.confirm('¿Estás seguro de eliminar esta regla?')) return;
    try {
      await commissionRulesApi.delete(id);
      toast.success('Regla eliminada');
      fetchData();
    } catch (error) {
      toast.error('Error al eliminar regla');
    }
  };

  const getAgencyName = (id) => agencies.find((a) => a.id === id)?.name || '-';
  const getRuleType = (type) => RULE_TYPES.find((t) => t.value === type);

  const formatValue = (rule) => {
    if (rule.rule_type === 'percentage' || rule.rule_type === 'fi_bonus') {
      return `${rule.value}%`;
    }
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0
    }).format(rule.value);
  };

  return (
    <div className="space-y-6" data-testid="commissions-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Reglas de Comisiones
          </h1>
          <p className="text-muted-foreground">
            Configura las reglas de comisión para vendedores por agencia
          </p>
        </div>
        <div className="flex gap-2">
          <Select value={selectedAgency} onValueChange={setSelectedAgency}>
            <SelectTrigger className="w-[200px]" data-testid="filter-agency-select">
              <SelectValue placeholder="Todas las agencias" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todas las agencias</SelectItem>
              {agencies.map((agency) => (
                <SelectItem key={agency.id} value={agency.id}>
                  {agency.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Dialog open={isDialogOpen} onOpenChange={(open) => {
            setIsDialogOpen(open);
            if (!open) {
              setEditingRule(null);
              setFormData({
                agency_id: '',
                name: '',
                rule_type: 'per_unit',
                value: '',
                min_units: '',
                max_units: ''
              });
            }
          }}>
            <DialogTrigger asChild>
              <Button className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="add-rule-btn">
                <Plus size={18} className="mr-2" />
                Nueva Regla
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>{editingRule ? 'Editar Regla' : 'Nueva Regla de Comisión'}</DialogTitle>
              </DialogHeader>
              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="agency_id">Agencia</Label>
                  <Select
                    value={formData.agency_id}
                    onValueChange={(value) => setFormData({ ...formData, agency_id: value })}
                    required
                  >
                    <SelectTrigger data-testid="rule-agency-select">
                      <SelectValue placeholder="Seleccionar agencia" />
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
                <div className="space-y-2">
                  <Label htmlFor="name">Nombre de la Regla</Label>
                  <Input
                    id="name"
                    value={formData.name}
                    onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                    placeholder="Ej: Comisión base por unidad"
                    required
                    data-testid="rule-name-input"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Tipo de Regla</Label>
                  <div className="grid grid-cols-2 gap-2">
                    {RULE_TYPES.map((type) => {
                      const Icon = type.icon;
                      return (
                        <button
                          key={type.value}
                          type="button"
                          onClick={() => setFormData({ ...formData, rule_type: type.value })}
                          className={`p-3 rounded-md border text-left transition-fast ${
                            formData.rule_type === type.value
                              ? 'border-[#002FA7] bg-[#002FA7]/5'
                              : 'border-border hover:border-[#002FA7]/50'
                          }`}
                          data-testid={`rule-type-${type.value}`}
                        >
                          <Icon size={20} weight="duotone" className={formData.rule_type === type.value ? 'text-[#002FA7]' : 'text-muted-foreground'} />
                          <div className="font-medium text-sm mt-1">{type.label}</div>
                          <div className="text-xs text-muted-foreground">{type.description}</div>
                        </button>
                      );
                    })}
                  </div>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="value">
                    {formData.rule_type === 'percentage' || formData.rule_type === 'fi_bonus' ? 'Porcentaje (%)' : 'Monto ($)'}
                  </Label>
                  <Input
                    id="value"
                    type="number"
                    step="0.01"
                    value={formData.value}
                    onChange={(e) => setFormData({ ...formData, value: e.target.value })}
                    placeholder={formData.rule_type === 'percentage' || formData.rule_type === 'fi_bonus' ? '2.5' : '5000'}
                    required
                    data-testid="rule-value-input"
                  />
                </div>
                {formData.rule_type === 'volume_bonus' && (
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="min_units">Mínimo Unidades</Label>
                      <Input
                        id="min_units"
                        type="number"
                        value={formData.min_units}
                        onChange={(e) => setFormData({ ...formData, min_units: e.target.value })}
                        placeholder="5"
                        data-testid="rule-min-units-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="max_units">Máximo Unidades</Label>
                      <Input
                        id="max_units"
                        type="number"
                        value={formData.max_units}
                        onChange={(e) => setFormData({ ...formData, max_units: e.target.value })}
                        placeholder="10"
                        data-testid="rule-max-units-input"
                      />
                    </div>
                  </div>
                )}
                <div className="flex justify-end gap-2">
                  <Button type="button" variant="outline" onClick={() => setIsDialogOpen(false)}>
                    Cancelar
                  </Button>
                  <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-rule-btn">
                    {editingRule ? 'Actualizar' : 'Crear'}
                  </Button>
                </div>
              </form>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {/* Rule Type Info */}
      <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {RULE_TYPES.map((type) => {
          const Icon = type.icon;
          return (
            <Card key={type.value} className="border-border/40">
              <CardContent className="p-4 flex items-start gap-3">
                <div className="w-10 h-10 rounded-md bg-[#002FA7]/10 flex items-center justify-center flex-shrink-0">
                  <Icon size={20} weight="duotone" className="text-[#002FA7]" />
                </div>
                <div>
                  <h3 className="font-medium text-sm">{type.label}</h3>
                  <p className="text-xs text-muted-foreground">{type.description}</p>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Table */}
      <Card className="border-border/40">
        <div className="table-wrapper">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Nombre</TableHead>
                <TableHead>Agencia</TableHead>
                <TableHead>Tipo</TableHead>
                <TableHead className="text-right">Valor</TableHead>
                <TableHead>Rango Unidades</TableHead>
                <TableHead className="w-24"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                [...Array(3)].map((_, i) => (
                  <TableRow key={i}>
                    {[...Array(6)].map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton className="h-5 w-full" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : rules.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="text-center py-12">
                    <CurrencyDollar size={48} className="mx-auto text-muted-foreground mb-4" />
                    <p className="text-muted-foreground">No hay reglas de comisión configuradas</p>
                    <p className="text-sm text-muted-foreground">Crea una regla para calcular comisiones automáticamente</p>
                  </TableCell>
                </TableRow>
              ) : (
                rules.map((rule) => {
                  const ruleType = getRuleType(rule.rule_type);
                  const Icon = ruleType?.icon || CurrencyDollar;
                  return (
                    <TableRow key={rule.id} data-testid={`rule-row-${rule.id}`}>
                      <TableCell className="font-medium">{rule.name}</TableCell>
                      <TableCell>{getAgencyName(rule.agency_id)}</TableCell>
                      <TableCell>
                        <Badge variant="outline" className="gap-1">
                          <Icon size={14} />
                          {ruleType?.label}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right tabular-nums font-medium">
                        {formatValue(rule)}
                      </TableCell>
                      <TableCell>
                        {rule.min_units || rule.max_units ? (
                          <span className="text-sm">
                            {rule.min_units || 0} - {rule.max_units || '∞'}
                          </span>
                        ) : (
                          '-'
                        )}
                      </TableCell>
                      <TableCell>
                        <div className="flex justify-end gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleEdit(rule)}
                            data-testid={`edit-rule-${rule.id}`}
                          >
                            <Pencil size={16} />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDelete(rule.id)}
                            className="text-destructive hover:text-destructive"
                            data-testid={`delete-rule-${rule.id}`}
                          >
                            <Trash size={16} />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}
