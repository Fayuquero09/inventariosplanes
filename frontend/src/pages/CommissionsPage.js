import { useState, useEffect, useCallback, useMemo } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { commissionRulesApi, commissionClosuresApi, commissionSimulatorApi, commissionMatrixApi, vehicleCatalogApi } from '../lib/api';
import { useHierarchicalFilters, HierarchicalFilters } from '../components/HierarchicalFilters';
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
  DialogTitle
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
  Pencil,
  Trash,
  CurrencyDollar,
  Percent,
  Package,
  Coin,
  Check,
  X,
  Calculator
} from '@phosphor-icons/react';
import { toast } from 'sonner';

const RULE_TYPES = [
  { value: 'per_unit', label: 'Por Unidad', icon: Package, description: 'Monto fijo por cada unidad vendida' },
  { value: 'percentage', label: 'Porcentaje', icon: Percent, description: 'Porcentaje del precio de venta' },
  { value: 'volume_bonus', label: 'Incentivo por Volumen', icon: CurrencyDollar, description: 'Incentivo al alcanzar cierto número de unidades' },
  { value: 'fi_bonus', label: 'Incentivo F&I', icon: Coin, description: 'Porcentaje sobre ingresos de F&I' }
];

const COMMISSION_PENDING = 'pending';
const COMMISSION_APPROVED = 'approved';
const COMMISSION_REJECTED = 'rejected';

const COMMISSION_PROPOSER_ROLES = ['agency_sales_manager'];
const COMMISSION_APPROVER_ROLES = ['agency_general_manager', 'agency_admin', 'agency_commercial_manager'];
const SIMULATOR_ALLOWED_ROLES = ['seller', ...COMMISSION_PROPOSER_ROLES, ...COMMISSION_APPROVER_ROLES];
const COMMISSION_MATRIX_EDITOR_ROLES = [
  'app_admin',
  'group_admin',
  'group_finance_manager',
  'brand_admin',
  'agency_sales_manager',
  'agency_general_manager',
  'agency_admin',
  'agency_commercial_manager',
];
const EMPTY_MATRIX_GENERAL = {
  global_percentage: 0,
  global_per_unit_bonus: 0,
  global_aged_61_90_bonus: 0,
  global_aged_90_plus_bonus: 0,
  volume_tiers: [],
};

const normalizeVolumeTiers = (tiers = []) => (
  (Array.isArray(tiers) ? tiers : [])
    .map((row) => {
      const minUnits = Number.parseInt(row?.min_units, 10);
      const hasMax = row?.max_units !== null && row?.max_units !== undefined && row?.max_units !== '';
      const parsedMax = hasMax ? Number.parseInt(row?.max_units, 10) : null;
      const maxUnits = Number.isFinite(parsedMax) && parsedMax > 0
        ? Math.max(Number.isFinite(minUnits) ? Math.max(1, minUnits) : 1, parsedMax)
        : null;
      const bonusPerUnit = Number(row?.bonus_per_unit || 0);
      if (!Number.isFinite(minUnits) || minUnits < 1 || !Number.isFinite(bonusPerUnit) || bonusPerUnit <= 0) {
        return null;
      }
      return {
        min_units: Math.max(1, minUnits),
        max_units: maxUnits,
        bonus_per_unit: Math.max(0, bonusPerUnit),
      };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (a.min_units !== b.min_units) return a.min_units - b.min_units;
      return (a.max_units ?? Number.MAX_SAFE_INTEGER) - (b.max_units ?? Number.MAX_SAFE_INTEGER);
    })
);

const MONTHS = [
  { value: 1, label: 'Enero' },
  { value: 2, label: 'Febrero' },
  { value: 3, label: 'Marzo' },
  { value: 4, label: 'Abril' },
  { value: 5, label: 'Mayo' },
  { value: 6, label: 'Junio' },
  { value: 7, label: 'Julio' },
  { value: 8, label: 'Agosto' },
  { value: 9, label: 'Septiembre' },
  { value: 10, label: 'Octubre' },
  { value: 11, label: 'Noviembre' },
  { value: 12, label: 'Diciembre' }
];

const STATUS_LABELS = {
  [COMMISSION_PENDING]: 'Pendiente',
  [COMMISSION_APPROVED]: 'Aprobado',
  [COMMISSION_REJECTED]: 'Rechazado'
};

const getStatusClass = (status) => {
  if (status === COMMISSION_APPROVED) return 'border-[#2A9D8F] text-[#2A9D8F]';
  if (status === COMMISSION_REJECTED) return 'border-[#E63946] text-[#E63946]';
  return 'border-[#E9C46A] text-[#8A6D1A]';
};

const normalizeStatus = (status) => {
  const value = String(status || '').toLowerCase();
  if ([COMMISSION_PENDING, COMMISSION_APPROVED, COMMISSION_REJECTED].includes(value)) return value;
  return COMMISSION_APPROVED;
};

export default function CommissionsPage() {
  const { user } = useAuth();
  const filters = useHierarchicalFilters({ includeSellers: true });
  const { getFilterParams, selectedBrand, selectedAgency, selectedSeller, brands, agencies } = filters;
  const [rules, setRules] = useState([]);
  const [closures, setClosures] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isRuleDialogOpen, setIsRuleDialogOpen] = useState(false);
  const [isClosureDialogOpen, setIsClosureDialogOpen] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [ruleFormData, setRuleFormData] = useState({
    agency_id: '',
    name: '',
    rule_type: 'per_unit',
    value: '',
    min_units: '',
    max_units: ''
  });
  const [closureFormData, setClosureFormData] = useState({
    agency_id: '',
    seller_id: '',
    month: new Date().getMonth() + 1,
    year: new Date().getFullYear()
  });
  const [simulatorFormData, setSimulatorFormData] = useState({
    agency_id: '',
    seller_id: '',
    target_commission: '',
    units: '',
    average_ticket: '',
    average_fi_revenue: ''
  });
  const [simulatorResult, setSimulatorResult] = useState(null);
  const [simulating, setSimulating] = useState(false);
  const [brandCatalogLoading, setBrandCatalogLoading] = useState(false);
  const [brandCatalogModels, setBrandCatalogModels] = useState([]);
  const [matrixLoading, setMatrixLoading] = useState(false);
  const [matrixSaving, setMatrixSaving] = useState(false);
  const [matrixGeneral, setMatrixGeneral] = useState(EMPTY_MATRIX_GENERAL);
  const [matrixModels, setMatrixModels] = useState([]);
  const [modelCommissionPctMap, setModelCommissionPctMap] = useState({});
  const [globalCommissionPctInput, setGlobalCommissionPctInput] = useState('0');

  const canProposeRules = COMMISSION_PROPOSER_ROLES.includes(user?.role);
  const canApproveRules = COMMISSION_APPROVER_ROLES.includes(user?.role);
  const canManageRules = canProposeRules || canApproveRules;
  const canUseSimulator = SIMULATOR_ALLOWED_ROLES.includes(user?.role);
  const canEditMatrix = COMMISSION_MATRIX_EDITOR_ROLES.includes(user?.role);
  const effectiveGlobalCommissionPct = useMemo(() => {
    const parsed = Number(globalCommissionPctInput);
    if (Number.isFinite(parsed)) return Math.max(0, parsed);
    return Number(matrixGeneral.global_percentage || 0);
  }, [globalCommissionPctInput, matrixGeneral.global_percentage]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = getFilterParams();
      const [rulesRes, closuresRes] = await Promise.all([
        commissionRulesApi.getAll(params),
        commissionClosuresApi.getAll(params)
      ]);
      setRules(rulesRes.data || []);
      setClosures(closuresRes.data || []);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [getFilterParams]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const resolvedAgencyId = useMemo(() => {
    if (selectedAgency !== 'all') return selectedAgency;
    if (user?.agency_id) return user.agency_id;
    if (agencies.length === 1) return agencies[0].id;
    return '';
  }, [selectedAgency, user?.agency_id, agencies]);

  const resolvedBrandName = useMemo(() => {
    if (selectedBrand !== 'all') {
      return brands.find((brand) => brand.id === selectedBrand)?.name || '';
    }
    if (resolvedAgencyId) {
      const agency = agencies.find((item) => item.id === resolvedAgencyId);
      if (!agency?.brand_id) return '';
      return brands.find((brand) => brand.id === agency.brand_id)?.name || '';
    }
    return '';
  }, [selectedBrand, resolvedAgencyId, brands, agencies]);

  useEffect(() => {
    const loadBrandCatalogModels = async () => {
      if (!resolvedBrandName) {
        setBrandCatalogModels([]);
        return;
      }
      try {
        setBrandCatalogLoading(true);
        const response = await vehicleCatalogApi.getModels(resolvedBrandName, { allYears: true });
        const items = Array.isArray(response?.data?.items) ? response.data.items : [];
        const models = items
          .map((item) => ({
            name: String(item?.name || '').trim(),
            min_msrp: Number(item?.min_msrp || 0),
          }))
          .filter((item) => item.name)
          .sort((a, b) => a.name.localeCompare(b.name, 'es-MX'));
        setBrandCatalogModels(models);
      } catch (error) {
        setBrandCatalogModels([]);
        const detail = error.response?.data?.detail;
        toast.error(typeof detail === 'string' ? detail : 'No se pudo cargar el listado de vehículos de la marca');
      } finally {
        setBrandCatalogLoading(false);
      }
    };

    loadBrandCatalogModels();
  }, [resolvedBrandName]);

  useEffect(() => {
    const loadCommissionMatrix = async () => {
      if (!resolvedAgencyId) {
        setMatrixGeneral(EMPTY_MATRIX_GENERAL);
        setMatrixModels([]);
        setModelCommissionPctMap({});
        setGlobalCommissionPctInput('0');
        return;
      }
      try {
        setMatrixLoading(true);
        const response = await commissionMatrixApi.get(resolvedAgencyId);
        const data = response?.data || {};
        const normalizedVolumeTiers = normalizeVolumeTiers(data?.general?.volume_tiers || []);
        const normalizedGeneral = {
          ...EMPTY_MATRIX_GENERAL,
          ...(data.general || {}),
          volume_tiers: normalizedVolumeTiers,
        };
        const normalizedModels = Array.isArray(data.models) ? data.models : [];
        const nextPctMap = {};
        normalizedModels.forEach((row) => {
          const modelName = String(row?.model || '').trim();
          if (!modelName) return;
          const fallbackPct = Number(normalizedGeneral.global_percentage || 0);
          const rowPct = Number(
            row?.model_percentage != null ? row.model_percentage : fallbackPct
          );
          nextPctMap[modelName] = Number.isFinite(rowPct) ? rowPct : fallbackPct;
        });
        setMatrixGeneral(normalizedGeneral);
        setMatrixModels(normalizedModels);
        setModelCommissionPctMap(nextPctMap);
        setGlobalCommissionPctInput(String(Number(normalizedGeneral.global_percentage || 0)));
      } catch (error) {
        setMatrixGeneral(EMPTY_MATRIX_GENERAL);
        setMatrixModels([]);
        setModelCommissionPctMap({});
        setGlobalCommissionPctInput('0');
        const detail = error.response?.data?.detail;
        toast.error(typeof detail === 'string' ? detail : 'No se pudo cargar la matriz de comisiones');
      } finally {
        setMatrixLoading(false);
      }
    };

    loadCommissionMatrix();
  }, [resolvedAgencyId]);

  const handleModelCommissionPctChange = (modelName, value) => {
    const normalizedName = String(modelName || '').trim();
    if (!normalizedName) return;
    setModelCommissionPctMap((prev) => {
      if (value === '') return { ...prev, [normalizedName]: '' };
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return prev;
      return { ...prev, [normalizedName]: Math.max(0, numeric) };
    });
  };

  const handleGlobalCommissionPctChange = (value) => {
    setGlobalCommissionPctInput(value);
    if (value === '') return;
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return;
    setMatrixGeneral((prev) => ({ ...prev, global_percentage: Math.max(0, numeric) }));
  };

  const handleApplyGlobalCommissionPctToAll = () => {
    if (!brandCatalogModels.length) return;
    const pct = effectiveGlobalCommissionPct;
    setModelCommissionPctMap((prev) => {
      const next = { ...prev };
      brandCatalogModels.forEach((item) => {
        const modelName = String(item?.name || '').trim();
        if (!modelName) return;
        next[modelName] = pct;
      });
      return next;
    });
    toast.success(`% general aplicado a ${brandCatalogModels.length} modelos`);
  };

  const handleAddVolumeTier = () => {
    setMatrixGeneral((prev) => ({
      ...prev,
      volume_tiers: [
        ...(Array.isArray(prev?.volume_tiers) ? prev.volume_tiers : []),
        { min_units: 1, max_units: null, bonus_per_unit: 0 },
      ],
    }));
  };

  const handleRemoveVolumeTier = (index) => {
    setMatrixGeneral((prev) => {
      const tiers = Array.isArray(prev?.volume_tiers) ? [...prev.volume_tiers] : [];
      tiers.splice(index, 1);
      return { ...prev, volume_tiers: tiers };
    });
  };

  const handleVolumeTierChange = (index, field, value) => {
    setMatrixGeneral((prev) => {
      const tiers = Array.isArray(prev?.volume_tiers) ? [...prev.volume_tiers] : [];
      if (!tiers[index]) return prev;
      const next = { ...tiers[index] };
      if (field === 'min_units') {
        if (value === '') {
          next.min_units = '';
        } else {
          const numeric = Number.parseInt(value, 10);
          if (!Number.isFinite(numeric)) return prev;
          next.min_units = Math.max(1, numeric);
        }
      } else if (field === 'max_units') {
        if (value === '') {
          next.max_units = null;
        } else {
          const numeric = Number.parseInt(value, 10);
          if (!Number.isFinite(numeric)) return prev;
          next.max_units = Math.max(1, numeric);
        }
      } else if (field === 'bonus_per_unit') {
        if (value === '') {
          next.bonus_per_unit = '';
        } else {
          const numeric = Number(value);
          if (!Number.isFinite(numeric)) return prev;
          next.bonus_per_unit = Math.max(0, numeric);
        }
      }
      tiers[index] = next;
      return { ...prev, volume_tiers: tiers };
    });
  };

  const handleSaveModelCommissions = async () => {
    if (!resolvedAgencyId) {
      toast.error('Selecciona una agencia para guardar la comisión por modelo');
      return;
    }
    if (!canEditMatrix) {
      toast.error('Tu rol no puede editar comisión por modelo');
      return;
    }
    try {
      setMatrixSaving(true);
      const globalPctForSave = effectiveGlobalCommissionPct;
      const modelMap = new Map(
        matrixModels
          .filter((row) => String(row?.model || '').trim())
          .map((row) => [String(row.model).trim().toLowerCase(), row])
      );
      const catalogNames = brandCatalogModels
        .map((item) => String(item?.name || '').trim())
        .filter(Boolean);
      const catalogSet = new Set(catalogNames.map((name) => name.toLowerCase()));

      const payloadModels = catalogNames.map((modelName) => {
        const previous = modelMap.get(modelName.toLowerCase()) || {};
        const fallbackPct = Number(
          previous?.model_percentage != null
            ? previous.model_percentage
            : globalPctForSave
        );
        const editedPct = modelCommissionPctMap[modelName];
        const modelPct = Number.isFinite(Number(editedPct))
          ? Math.max(0, Number(editedPct))
          : Math.max(0, fallbackPct);
        return {
          model: modelName,
          model_percentage: modelPct,
          model_bonus: Number(previous?.model_bonus || 0),
          aged_61_90_bonus: Number(previous?.aged_61_90_bonus || 0),
          aged_90_plus_bonus: Number(previous?.aged_90_plus_bonus || 0),
          plant_incentive_share_pct: Number(previous?.plant_incentive_share_pct ?? 100),
        };
      });

      matrixModels.forEach((row) => {
        const modelName = String(row?.model || '').trim();
        if (!modelName) return;
        if (catalogSet.has(modelName.toLowerCase())) return;
        const editedPct = modelCommissionPctMap[modelName];
        const fallbackPct = Number(
          row?.model_percentage != null
            ? row.model_percentage
            : globalPctForSave
        );
        payloadModels.push({
          model: modelName,
          model_percentage: Number.isFinite(Number(editedPct))
            ? Math.max(0, Number(editedPct))
            : Math.max(0, fallbackPct),
          model_bonus: Number(row?.model_bonus || 0),
          aged_61_90_bonus: Number(row?.aged_61_90_bonus || 0),
          aged_90_plus_bonus: Number(row?.aged_90_plus_bonus || 0),
          plant_incentive_share_pct: Number(row?.plant_incentive_share_pct ?? 100),
        });
      });

      const payloadGeneral = {
        global_percentage: Number(globalPctForSave),
        global_per_unit_bonus: Number(matrixGeneral.global_per_unit_bonus || 0),
        global_aged_61_90_bonus: Number(matrixGeneral.global_aged_61_90_bonus || 0),
        global_aged_90_plus_bonus: Number(matrixGeneral.global_aged_90_plus_bonus || 0),
        volume_tiers: normalizeVolumeTiers(matrixGeneral.volume_tiers),
      };

      const response = await commissionMatrixApi.upsert({
        agency_id: resolvedAgencyId,
        general: payloadGeneral,
        models: payloadModels,
      });

      const savedGeneral = {
        ...EMPTY_MATRIX_GENERAL,
        ...((response?.data?.general) || payloadGeneral),
        volume_tiers: normalizeVolumeTiers((response?.data?.general?.volume_tiers) || payloadGeneral.volume_tiers),
      };
      const savedModels = Array.isArray(response?.data?.models) ? response.data.models : payloadModels;
      const nextPctMap = {};
      savedModels.forEach((row) => {
        const modelName = String(row?.model || '').trim();
        if (!modelName) return;
        const fallbackPct = Number(savedGeneral.global_percentage || 0);
        const value = Number(row?.model_percentage != null ? row.model_percentage : fallbackPct);
        nextPctMap[modelName] = Number.isFinite(value) ? value : fallbackPct;
      });
      setMatrixGeneral(savedGeneral);
      setGlobalCommissionPctInput(String(Number(savedGeneral.global_percentage || 0)));
      setMatrixModels(savedModels);
      setModelCommissionPctMap(nextPctMap);
      toast.success('% de comisión por modelo guardado');
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo guardar la comisión por modelo');
    } finally {
      setMatrixSaving(false);
    }
  };

  const resetRuleForm = useCallback(() => {
    setRuleFormData({
      agency_id: selectedAgency !== 'all' ? selectedAgency : '',
      name: '',
      rule_type: 'per_unit',
      value: '',
      min_units: '',
      max_units: ''
    });
  }, [selectedAgency]);

  const openNewRuleDialog = () => {
    setEditingRule(null);
    resetRuleForm();
    setIsRuleDialogOpen(true);
  };

  const handleRuleSubmit = async (e) => {
    e.preventDefault();
    if (!canProposeRules) {
      toast.error('Solo Gerencia de Ventas puede proponer reglas');
      return;
    }

    try {
      const data = {
        ...ruleFormData,
        value: parseFloat(ruleFormData.value),
        min_units: ruleFormData.min_units ? parseInt(ruleFormData.min_units, 10) : null,
        max_units: ruleFormData.max_units ? parseInt(ruleFormData.max_units, 10) : null
      };

      if (editingRule) {
        await commissionRulesApi.update(editingRule.id, data);
        toast.success('Regla actualizada y enviada a aprobación');
      } else {
        await commissionRulesApi.create(data);
        toast.success('Regla creada y enviada a aprobación');
      }

      setIsRuleDialogOpen(false);
      setEditingRule(null);
      resetRuleForm();
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al guardar regla');
    }
  };

  const handleEditRule = (rule) => {
    setEditingRule(rule);
    setRuleFormData({
      agency_id: rule.agency_id,
      name: rule.name,
      rule_type: rule.rule_type,
      value: String(rule.value ?? ''),
      min_units: rule.min_units != null ? String(rule.min_units) : '',
      max_units: rule.max_units != null ? String(rule.max_units) : ''
    });
    setIsRuleDialogOpen(true);
  };

  const handleDeleteRule = async (rule) => {
    if (!rule?.id) return;
    if (!window.confirm('¿Eliminar esta regla?')) return;
    try {
      await commissionRulesApi.delete(rule.id);
      toast.success('Regla eliminada');
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al eliminar regla');
    }
  };

  const handleRuleApprovalDecision = async (rule, decision) => {
    if (!rule?.id) return;
    const normalizedDecision = String(decision || '').toLowerCase();
    if (![COMMISSION_APPROVED, COMMISSION_REJECTED].includes(normalizedDecision)) return;

    let comment = null;
    if (normalizedDecision === COMMISSION_REJECTED) {
      const reason = window.prompt('Motivo de rechazo (obligatorio):', '');
      if (reason === null) return;
      comment = String(reason || '').trim();
      if (!comment) {
        toast.error('El rechazo requiere un motivo');
        return;
      }
    }

    try {
      await commissionRulesApi.approve(rule.id, { decision: normalizedDecision, comment });
      toast.success(normalizedDecision === COMMISSION_APPROVED ? 'Regla aprobada' : 'Regla rechazada');
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo procesar la aprobación');
    }
  };

  const openClosureDialog = () => {
    setClosureFormData({
      agency_id: selectedAgency !== 'all' ? selectedAgency : user?.agency_id || '',
      seller_id: selectedSeller !== 'all' ? selectedSeller : '',
      month: new Date().getMonth() + 1,
      year: new Date().getFullYear()
    });
    setIsClosureDialogOpen(true);
  };

  const handleCreateClosure = async (e) => {
    e.preventDefault();
    if (!canProposeRules) {
      toast.error('Solo Gerencia de Ventas puede proponer cierres');
      return;
    }
    try {
      if (!closureFormData.agency_id || !closureFormData.seller_id) {
        toast.error('Selecciona dealer y vendedor');
        return;
      }
      await commissionClosuresApi.create({
        agency_id: closureFormData.agency_id,
        seller_id: closureFormData.seller_id,
        month: parseInt(closureFormData.month, 10),
        year: parseInt(closureFormData.year, 10)
      });
      toast.success('Cierre mensual generado y enviado a aprobación');
      setIsClosureDialogOpen(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al crear cierre');
    }
  };

  const handleClosureApprovalDecision = async (closure, decision) => {
    if (!closure?.id) return;
    const normalizedDecision = String(decision || '').toLowerCase();
    if (![COMMISSION_APPROVED, COMMISSION_REJECTED].includes(normalizedDecision)) return;

    let comment = null;
    if (normalizedDecision === COMMISSION_REJECTED) {
      const reason = window.prompt('Motivo de rechazo (obligatorio):', '');
      if (reason === null) return;
      comment = String(reason || '').trim();
      if (!comment) {
        toast.error('El rechazo requiere un motivo');
        return;
      }
    }

    try {
      await commissionClosuresApi.approve(closure.id, { decision: normalizedDecision, comment });
      toast.success(normalizedDecision === COMMISSION_APPROVED ? 'Cierre aprobado' : 'Cierre rechazado');
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo procesar la aprobación');
    }
  };

  const handleSimulatorSubmit = async (e) => {
    e.preventDefault();
    if (!canUseSimulator) {
      toast.error('Tu rol no puede usar el simulador');
      return;
    }

    const agencyId = simulatorFormData.agency_id || (selectedAgency !== 'all' ? selectedAgency : user?.agency_id || '');
    if (!agencyId) {
      toast.error('Selecciona un dealer para simular');
      return;
    }

    const payload = {
      agency_id: agencyId,
      seller_id: user?.role === 'seller' ? undefined : (simulatorFormData.seller_id || undefined),
      target_commission: parseFloat(simulatorFormData.target_commission || '0'),
      units: parseInt(simulatorFormData.units || '0', 10),
      average_ticket: parseFloat(simulatorFormData.average_ticket || '0'),
      average_fi_revenue: parseFloat(simulatorFormData.average_fi_revenue || '0')
    };

    try {
      setSimulating(true);
      const res = await commissionSimulatorApi.simulate(payload);
      setSimulatorResult(res.data);
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo simular');
    } finally {
      setSimulating(false);
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0
    }).format(value || 0);
  };

  const formatRuleValue = (rule) => {
    if (rule.rule_type === 'percentage' || rule.rule_type === 'fi_bonus') {
      return `${rule.value}%`;
    }
    return formatCurrency(rule.value);
  };

  const rulesByAgency = rules.reduce((acc, rule) => {
    const key = rule.agency_id || 'general';
    if (!acc[key]) {
      acc[key] = {
        agency_name: rule.agency_name || 'General',
        brand_name: rule.brand_name,
        group_name: rule.group_name,
        rules: []
      };
    }
    acc[key].rules.push(rule);
    return acc;
  }, {});

  return (
    <div className="space-y-6" data-testid="commissions-page">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Comisiones e Incentivos
          </h1>
          <p className="text-muted-foreground">
            Ventas propone reglas/cierres y Gerencia General aprueba.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          {canProposeRules && (
            <Button onClick={openNewRuleDialog} className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="add-rule-btn">
              <Plus size={18} className="mr-2" />
              Nueva Regla
            </Button>
          )}
          {canProposeRules && (
            <Button variant="outline" onClick={openClosureDialog} data-testid="create-closure-btn">
              <Plus size={18} className="mr-2" />
              Cierre Mensual
            </Button>
          )}
        </div>
      </div>

      <HierarchicalFilters filters={filters} includeSellers={true} />

      <Card className="border-border/40" data-testid="brand-vehicles-list-card">
        <CardHeader>
          <CardTitle>Vehículos de la Marca</CardTitle>
          <CardDescription>
            Listado de modelos disponibles en catálogo para la marca seleccionada.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {!resolvedBrandName ? (
            <div className="text-sm text-muted-foreground">
              Selecciona una marca (o una agencia) para ver sus vehículos.
            </div>
          ) : brandCatalogLoading ? (
            <Skeleton className="h-56 w-full" />
          ) : brandCatalogModels.length === 0 ? (
            <div className="text-sm text-muted-foreground">
              No hay modelos cargados para {resolvedBrandName}.
            </div>
          ) : (
            <div className="space-y-3">
              <div className="text-sm text-muted-foreground">
                Marca: <span className="font-medium text-foreground">{resolvedBrandName}</span> · {brandCatalogModels.length} modelos
              </div>
              <div className="text-xs text-muted-foreground">
                {resolvedAgencyId
                  ? 'Comisión por modelo aplicada a la agencia seleccionada.'
                  : 'Selecciona una agencia para capturar el % de comisión por modelo.'}
              </div>
              {canEditMatrix && resolvedAgencyId && (
                <div className="rounded-md border border-border/40 p-3 bg-muted/20 space-y-4">
                  <div className="flex flex-col md:flex-row md:items-end gap-3">
                    <div className="space-y-1">
                      <Label htmlFor="global-commission-pct">% general comisión vendedor</Label>
                      <div className="flex items-center gap-2">
                        <Input
                          id="global-commission-pct"
                          type="number"
                          min="0"
                          step="0.1"
                          value={globalCommissionPctInput}
                          onChange={(e) => handleGlobalCommissionPctChange(e.target.value)}
                          className="w-28 text-right"
                          data-testid="global-commission-pct-input"
                        />
                        <span className="text-muted-foreground">%</span>
                      </div>
                    </div>
                    <Button
                      type="button"
                      variant="outline"
                      onClick={handleApplyGlobalCommissionPctToAll}
                      disabled={matrixLoading || brandCatalogLoading || !brandCatalogModels.length}
                      data-testid="apply-global-commission-to-models-btn"
                    >
                      Aplicar a todos los modelos
                    </Button>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <Label>Incentivo escalonado por volumen mensual (por vendedor)</Label>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        onClick={handleAddVolumeTier}
                        data-testid="add-volume-tier-btn"
                      >
                        <Plus size={14} className="mr-1" />
                        Agregar tramo
                      </Button>
                    </div>
                    {(matrixGeneral.volume_tiers || []).length === 0 ? (
                      <div className="text-xs text-muted-foreground">
                        Sin tramos. Ejemplo: 4-5 unidades = $300 por unidad, 6-7 unidades = $600 por unidad.
                      </div>
                    ) : (
                      <div className="space-y-2">
                        {(matrixGeneral.volume_tiers || []).map((tier, index) => (
                          <div
                            key={`volume-tier-${index}`}
                            className="grid grid-cols-1 md:grid-cols-[1fr_1fr_1fr_auto] gap-2 items-end"
                          >
                            <div className="space-y-1">
                              <Label className="text-xs">Mín unidades</Label>
                              <Input
                                type="number"
                                min="1"
                                step="1"
                                value={tier?.min_units ?? ''}
                                onChange={(e) => handleVolumeTierChange(index, 'min_units', e.target.value)}
                                data-testid={`volume-tier-min-${index}`}
                              />
                            </div>
                            <div className="space-y-1">
                              <Label className="text-xs">Máx unidades (opcional)</Label>
                              <Input
                                type="number"
                                min="1"
                                step="1"
                                value={tier?.max_units ?? ''}
                                onChange={(e) => handleVolumeTierChange(index, 'max_units', e.target.value)}
                                placeholder="Sin límite"
                                data-testid={`volume-tier-max-${index}`}
                              />
                            </div>
                            <div className="space-y-1">
                              <Label className="text-xs">Incentivo $ por unidad</Label>
                              <Input
                                type="number"
                                min="0"
                                step="0.01"
                                value={tier?.bonus_per_unit ?? ''}
                                onChange={(e) => handleVolumeTierChange(index, 'bonus_per_unit', e.target.value)}
                                data-testid={`volume-tier-bonus-${index}`}
                              />
                            </div>
                            <Button
                              type="button"
                              variant="outline"
                              size="icon"
                              onClick={() => handleRemoveVolumeTier(index)}
                              data-testid={`remove-volume-tier-${index}`}
                            >
                              <Trash size={14} />
                            </Button>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              )}
              <div className="rounded-md border border-border/40 overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Modelo</TableHead>
                      <TableHead className="text-right">Min MSRP</TableHead>
                      <TableHead className="text-right">% comisión vendedor</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {brandCatalogModels.map((model) => (
                      <TableRow key={model.name}>
                        <TableCell className="font-medium">{model.name}</TableCell>
                        <TableCell className="text-right">{formatCurrency(model.min_msrp || 0)}</TableCell>
                        <TableCell className="text-right">
                          {matrixLoading ? (
                            <span className="text-muted-foreground">...</span>
                          ) : canEditMatrix && resolvedAgencyId ? (
                            <div className="flex items-center justify-end gap-2">
                              <Input
                                type="number"
                                min="0"
                                step="0.1"
                                value={
                                  modelCommissionPctMap[model.name] ?? effectiveGlobalCommissionPct
                                }
                                onChange={(e) => handleModelCommissionPctChange(model.name, e.target.value)}
                                className="w-24 text-right"
                                data-testid={`commission-model-pct-${model.name}`}
                              />
                              <span className="text-muted-foreground">%</span>
                            </div>
                          ) : (
                            `${Number(
                              modelCommissionPctMap[model.name] ?? effectiveGlobalCommissionPct
                            ).toFixed(1)}%`
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              {canEditMatrix && resolvedAgencyId && (
                <div className="flex justify-end">
                  <Button
                    onClick={handleSaveModelCommissions}
                    disabled={matrixSaving || matrixLoading || brandCatalogLoading}
                    className="bg-[#002FA7] hover:bg-[#002FA7]/90"
                    data-testid="save-model-commission-pct-btn"
                  >
                    {matrixSaving ? 'Guardando...' : 'Guardar % comisión vendedor'}
                  </Button>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      {!canManageRules && (
        <Card className="border-border/40">
          <CardContent className="py-4 text-sm text-muted-foreground">
            Este rol tiene acceso de consulta en reglas y cierres de comisiones.
          </CardContent>
        </Card>
      )}

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

      {loading ? (
        <Card className="border-border/40">
          <CardContent className="p-4">
            <Skeleton className="h-64 w-full" />
          </CardContent>
        </Card>
      ) : Object.keys(rulesByAgency).length === 0 ? (
        <Card className="border-border/40">
          <CardContent className="text-center py-12">
            <CurrencyDollar size={48} className="mx-auto text-muted-foreground mb-4" />
            <p className="text-muted-foreground">No hay reglas de comisión configuradas</p>
          </CardContent>
        </Card>
      ) : (
        Object.entries(rulesByAgency).map(([agencyId, data]) => (
          <Card key={agencyId} className="border-border/40">
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-lg">{data.agency_name}</CardTitle>
                <div className="text-sm text-muted-foreground">{data.brand_name} • {data.group_name}</div>
              </div>
            </CardHeader>
            <div className="table-wrapper">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Nombre</TableHead>
                    <TableHead>Tipo</TableHead>
                    <TableHead className="text-right">Valor</TableHead>
                    <TableHead>Rango</TableHead>
                    <TableHead>Estatus</TableHead>
                    <TableHead className="w-[260px]"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.rules.map((rule) => {
                    const status = normalizeStatus(rule.approval_status);
                    const ruleType = RULE_TYPES.find((t) => t.value === rule.rule_type);
                    const Icon = ruleType?.icon || CurrencyDollar;
                    return (
                      <TableRow key={rule.id} data-testid={`rule-row-${rule.id}`}>
                        <TableCell className="font-medium">{rule.name}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className="gap-1">
                            <Icon size={12} />
                            {ruleType?.label || rule.rule_type}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right">{formatRuleValue(rule)}</TableCell>
                        <TableCell>
                          {rule.rule_type === 'volume_bonus'
                            ? `${rule.min_units || 0} - ${rule.max_units || '∞'} unidades`
                            : '-'}
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline" className={getStatusClass(status)}>
                            {STATUS_LABELS[status]}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex flex-wrap justify-end gap-2">
                            {canProposeRules && (
                              <>
                                <Button size="sm" variant="outline" onClick={() => handleEditRule(rule)} data-testid={`edit-rule-${rule.id}`}>
                                  <Pencil size={14} className="mr-1" />
                                  Editar
                                </Button>
                                <Button size="sm" variant="destructive" onClick={() => handleDeleteRule(rule)} data-testid={`delete-rule-${rule.id}`}>
                                  <Trash size={14} className="mr-1" />
                                  Borrar
                                </Button>
                              </>
                            )}
                            {canApproveRules && status === COMMISSION_PENDING && (
                              <>
                                <Button size="sm" variant="outline" onClick={() => handleRuleApprovalDecision(rule, COMMISSION_REJECTED)}>
                                  <X size={14} className="mr-1" />
                                  Rechazar
                                </Button>
                                <Button size="sm" className="bg-[#2A9D8F] hover:bg-[#2A9D8F]/90" onClick={() => handleRuleApprovalDecision(rule, COMMISSION_APPROVED)}>
                                  <Check size={14} className="mr-1" />
                                  Aprobar
                                </Button>
                              </>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          </Card>
        ))
      )}

      <Card className="border-border/40">
        <CardHeader>
          <CardTitle>Cierres Mensuales por Vendedor</CardTitle>
          <CardDescription>Snapshot mensual de comisiones con aprobación operativa.</CardDescription>
        </CardHeader>
        <CardContent>
          {loading ? (
            <Skeleton className="h-48 w-full" />
          ) : closures.length === 0 ? (
            <p className="text-sm text-muted-foreground">Sin cierres registrados.</p>
          ) : (
            <div className="table-wrapper">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Vendedor</TableHead>
                    <TableHead>Dealer</TableHead>
                    <TableHead>Periodo</TableHead>
                    <TableHead className="text-right">Comisión</TableHead>
                    <TableHead>Estatus</TableHead>
                    <TableHead className="w-[220px]"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {closures.map((closure) => {
                    const status = normalizeStatus(closure.approval_status);
                    const snapshot = closure.snapshot || {};
                    return (
                      <TableRow key={closure.id}>
                        <TableCell>{closure.seller_name || closure.seller_id}</TableCell>
                        <TableCell>{closure.agency_name || closure.agency_id}</TableCell>
                        <TableCell>{MONTHS.find((m) => m.value === closure.month)?.label || closure.month} {closure.year}</TableCell>
                        <TableCell className="text-right">{formatCurrency(snapshot.commission_total || 0)}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className={getStatusClass(status)}>
                            {STATUS_LABELS[status]}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="flex justify-end gap-2">
                            {canApproveRules && status === COMMISSION_PENDING && (
                              <>
                                <Button size="sm" variant="outline" onClick={() => handleClosureApprovalDecision(closure, COMMISSION_REJECTED)}>
                                  <X size={14} className="mr-1" />
                                  Rechazar
                                </Button>
                                <Button size="sm" className="bg-[#2A9D8F] hover:bg-[#2A9D8F]/90" onClick={() => handleClosureApprovalDecision(closure, COMMISSION_APPROVED)}>
                                  <Check size={14} className="mr-1" />
                                  Aprobar
                                </Button>
                              </>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {canUseSimulator && (
        <Card className="border-border/40" data-testid="commission-simulator-card">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Calculator size={20} />
              Simulador de Comisiones (What-if)
            </CardTitle>
            <CardDescription>
              Ajusta números para estimar qué necesitas vender para alcanzar tu meta, sin tocar objetivos reales.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form onSubmit={handleSimulatorSubmit} className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              <div className="space-y-2">
                <Label>Meta de comisión (MXN)</Label>
                <Input
                  type="number"
                  min="0"
                  value={simulatorFormData.target_commission}
                  onChange={(e) => setSimulatorFormData((prev) => ({ ...prev, target_commission: e.target.value }))}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label>Unidades simuladas</Label>
                <Input
                  type="number"
                  min="0"
                  value={simulatorFormData.units}
                  onChange={(e) => setSimulatorFormData((prev) => ({ ...prev, units: e.target.value }))}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label>Ticket promedio (MXN)</Label>
                <Input
                  type="number"
                  min="0"
                  value={simulatorFormData.average_ticket}
                  onChange={(e) => setSimulatorFormData((prev) => ({ ...prev, average_ticket: e.target.value }))}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label>Ingreso F&I promedio (MXN)</Label>
                <Input
                  type="number"
                  min="0"
                  value={simulatorFormData.average_fi_revenue}
                  onChange={(e) => setSimulatorFormData((prev) => ({ ...prev, average_fi_revenue: e.target.value }))}
                  required
                />
              </div>
              <div className="space-y-2">
                <Label>Dealer</Label>
                <Select
                  value={simulatorFormData.agency_id || (selectedAgency !== 'all' ? selectedAgency : user?.agency_id || '')}
                  onValueChange={(value) => setSimulatorFormData((prev) => ({ ...prev, agency_id: value }))}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Selecciona dealer" />
                  </SelectTrigger>
                  <SelectContent>
                    {(filters.filteredAgencies || []).map((agency) => (
                      <SelectItem key={agency.id} value={agency.id}>
                        {agency.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              {user?.role !== 'seller' && (
                <div className="space-y-2">
                  <Label>Vendedor (opcional)</Label>
                  <Select
                    value={simulatorFormData.seller_id || 'none'}
                    onValueChange={(value) => setSimulatorFormData((prev) => ({ ...prev, seller_id: value === 'none' ? '' : value }))}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Selecciona vendedor" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="none">Sin vendedor específico</SelectItem>
                      {(filters.sellers || []).map((seller) => (
                        <SelectItem key={seller.id} value={seller.id}>
                          {seller.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              )}
              <div className="md:col-span-2 lg:col-span-3 flex justify-end">
                <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" disabled={simulating}>
                  {simulating ? 'Simulando...' : 'Calcular Escenario'}
                </Button>
              </div>
            </form>

            {simulatorResult && (
              <div className="mt-4 grid gap-3 sm:grid-cols-3">
                <Card className="border-border/40">
                  <CardContent className="p-4">
                    <div className="text-xs uppercase tracking-wider text-muted-foreground">Comisión estimada</div>
                    <div className="text-xl font-bold">{formatCurrency(simulatorResult.estimated_commission)}</div>
                  </CardContent>
                </Card>
                <Card className="border-border/40">
                  <CardContent className="p-4">
                    <div className="text-xs uppercase tracking-wider text-muted-foreground">Diferencia vs meta</div>
                    <div className="text-xl font-bold">{formatCurrency(simulatorResult.difference_vs_target)}</div>
                  </CardContent>
                </Card>
                <Card className="border-border/40">
                  <CardContent className="p-4">
                    <div className="text-xs uppercase tracking-wider text-muted-foreground">Unidades sugeridas</div>
                    <div className="text-xl font-bold">{simulatorResult.suggested_units_to_target ?? 'N/A'}</div>
                  </CardContent>
                </Card>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      <Dialog open={isRuleDialogOpen} onOpenChange={setIsRuleDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingRule ? 'Editar Regla' : 'Nueva Regla de Comisión'}</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleRuleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label>Dealer (Agencia)</Label>
              <Select
                value={ruleFormData.agency_id}
                onValueChange={(value) => setRuleFormData((prev) => ({ ...prev, agency_id: value }))}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona dealer" />
                </SelectTrigger>
                <SelectContent>
                  {(filters.filteredAgencies || []).map((agency) => (
                    <SelectItem key={agency.id} value={agency.id}>
                      {agency.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Nombre</Label>
              <Input
                value={ruleFormData.name}
                onChange={(e) => setRuleFormData((prev) => ({ ...prev, name: e.target.value }))}
                required
              />
            </div>
            <div className="space-y-2">
              <Label>Tipo</Label>
              <Select
                value={ruleFormData.rule_type}
                onValueChange={(value) => setRuleFormData((prev) => ({ ...prev, rule_type: value }))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {RULE_TYPES.map((type) => (
                    <SelectItem key={type.value} value={type.value}>
                      {type.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Valor</Label>
              <Input
                type="number"
                step="0.01"
                min="0"
                value={ruleFormData.value}
                onChange={(e) => setRuleFormData((prev) => ({ ...prev, value: e.target.value }))}
                required
              />
            </div>
            {ruleFormData.rule_type === 'volume_bonus' && (
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-2">
                  <Label>Min unidades</Label>
                  <Input
                    type="number"
                    min="0"
                    value={ruleFormData.min_units}
                    onChange={(e) => setRuleFormData((prev) => ({ ...prev, min_units: e.target.value }))}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Max unidades</Label>
                  <Input
                    type="number"
                    min="0"
                    value={ruleFormData.max_units}
                    onChange={(e) => setRuleFormData((prev) => ({ ...prev, max_units: e.target.value }))}
                  />
                </div>
              </div>
            )}
            <div className="flex justify-end gap-2">
              <Button type="button" variant="outline" onClick={() => setIsRuleDialogOpen(false)}>
                Cancelar
              </Button>
              <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90">
                {editingRule ? 'Guardar Cambios' : 'Enviar a Aprobación'}
              </Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={isClosureDialogOpen} onOpenChange={setIsClosureDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Generar Cierre Mensual</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleCreateClosure} className="space-y-4">
            <div className="space-y-2">
              <Label>Dealer (Agencia)</Label>
              <Select
                value={closureFormData.agency_id}
                onValueChange={(value) => setClosureFormData((prev) => ({ ...prev, agency_id: value }))}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona dealer" />
                </SelectTrigger>
                <SelectContent>
                  {(filters.filteredAgencies || []).map((agency) => (
                    <SelectItem key={agency.id} value={agency.id}>
                      {agency.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Vendedor</Label>
              <Select
                value={closureFormData.seller_id || 'none'}
                onValueChange={(value) => setClosureFormData((prev) => ({ ...prev, seller_id: value === 'none' ? '' : value }))}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Selecciona vendedor" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Selecciona vendedor</SelectItem>
                  {(filters.sellers || []).map((seller) => (
                    <SelectItem key={seller.id} value={seller.id}>
                      {seller.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div className="space-y-2">
                <Label>Mes</Label>
                <Select
                  value={String(closureFormData.month)}
                  onValueChange={(value) => setClosureFormData((prev) => ({ ...prev, month: parseInt(value, 10) }))}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {MONTHS.map((month) => (
                      <SelectItem key={month.value} value={String(month.value)}>
                        {month.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Año</Label>
                <Input
                  type="number"
                  min="2024"
                  max="2100"
                  value={closureFormData.year}
                  onChange={(e) => setClosureFormData((prev) => ({ ...prev, year: parseInt(e.target.value || '0', 10) || new Date().getFullYear() }))}
                  required
                />
              </div>
            </div>
            <div className="flex justify-end gap-2">
              <Button type="button" variant="outline" onClick={() => setIsClosureDialogOpen(false)}>
                Cancelar
              </Button>
              <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90">
                Generar y Enviar
              </Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
