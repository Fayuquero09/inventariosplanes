import { useState, useEffect, useCallback, useMemo } from 'react';
import { salesObjectivesApi, dashboardApi, vehicleCatalogApi, sellersApi } from '../lib/api';
import { useHierarchicalFilters, HierarchicalFilters } from '../components/HierarchicalFilters';
import { useAuth } from '../contexts/AuthContext';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import SafeChart from '../components/SafeChart';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Skeleton } from '../components/ui/skeleton';
import { Progress } from '../components/ui/progress';
import { Target, Users, Storefront } from '@phosphor-icons/react';
import { toast } from 'sonner';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from 'recharts';

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

const OBJECTIVE_DRAFT = 'draft';
const OBJECTIVE_PENDING = 'pending';
const OBJECTIVE_APPROVED = 'approved';
const OBJECTIVE_REJECTED = 'rejected';

const OBJECTIVE_EDITOR_ROLES = ['agency_sales_manager'];
const AGENCY_SCOPED_ROLES = ['agency_sales_manager', 'agency_general_manager', 'agency_admin', 'agency_commercial_manager', 'agency_user', 'seller'];

const OBJECTIVE_STATUS_LABELS = {
  [OBJECTIVE_DRAFT]: 'Borrador',
  [OBJECTIVE_PENDING]: 'Activo',
  [OBJECTIVE_APPROVED]: 'Activo',
  [OBJECTIVE_REJECTED]: 'Activo'
};

const CAPTURE_SCOPE = {
  AGENCY: 'agency',
  SELLER: 'seller',
};

const CAPTURE_MODE = {
  TOTAL: 'total',
  MODEL: 'model',
};

export default function ObjectivesPage() {
  const { user } = useAuth();
  const filters = useHierarchicalFilters({ includeSellers: false });
  const { getFilterParams } = filters;
  const [objectives, setObjectives] = useState([]);
  const [sellerPerformance, setSellerPerformance] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedMonth, setSelectedMonth] = useState(new Date().getMonth() + 1);
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [availableModels, setAvailableModels] = useState([]);
  const [loadingModels, setLoadingModels] = useState(false);
  const [availableSellers, setAvailableSellers] = useState([]);
  const [loadingSellers, setLoadingSellers] = useState(false);
  const [modelSearch, setModelSearch] = useState('');
  const [modelTargets, setModelTargets] = useState({});
  const [captureScope, setCaptureScope] = useState(CAPTURE_SCOPE.AGENCY);
  const [captureMode, setCaptureMode] = useState(CAPTURE_MODE.MODEL);
  const [totalUnitsInput, setTotalUnitsInput] = useState('');
  const [totalRevenueInput, setTotalRevenueInput] = useState('');
  const [loadingSuggestion, setLoadingSuggestion] = useState(false);
  const [lastSuggestionMeta, setLastSuggestionMeta] = useState(null);
  const [formData, setFormData] = useState({
    agency_id: '',
    seller_id: '',
    month: new Date().getMonth() + 1,
    year: new Date().getFullYear()
  });

  const canCreateObjectives = OBJECTIVE_EDITOR_ROLES.includes(user?.role);
  const objectiveSubmitLabel = 'Guardar y Aplicar';
  const isAgencyScopedUser = AGENCY_SCOPED_ROLES.includes(user?.role);
  const singleAgencyId = filters.filteredAgencies.length === 1 ? filters.filteredAgencies[0]?.id : '';
  const shouldLockAgencySelector = isAgencyScopedUser && Boolean(singleAgencyId);

  useEffect(() => {
    if (!shouldLockAgencySelector || !singleAgencyId) return;
    setFormData((prev) => (
      prev.agency_id === singleAgencyId ? prev : { ...prev, agency_id: singleAgencyId }
    ));
  }, [shouldLockAgencySelector, singleAgencyId]);

  useEffect(() => {
    if (filters.selectedAgency === 'all') return;
    setFormData((prev) => (
      prev.agency_id === filters.selectedAgency ? prev : { ...prev, agency_id: filters.selectedAgency }
    ));
  }, [filters.selectedAgency]);

  useEffect(() => {
    setFormData((prev) => {
      if (prev.month === selectedMonth && prev.year === selectedYear) return prev;
      return { ...prev, month: selectedMonth, year: selectedYear };
    });
  }, [selectedMonth, selectedYear]);

  useEffect(() => {
    if (captureScope === CAPTURE_SCOPE.SELLER) return;
    setFormData((prev) => (prev.seller_id ? { ...prev, seller_id: '' } : prev));
  }, [captureScope]);

  useEffect(() => {
    setLastSuggestionMeta(null);
  }, [captureScope, captureMode, formData.agency_id, formData.seller_id, formData.month, formData.year]);

  const loadAgencyModels = useCallback(async (agencyId) => {
    if (!agencyId) {
      setAvailableModels([]);
      setModelSearch('');
      setModelTargets({});
      return;
    }

    const agency = (filters.agencies || []).find((item) => item.id === agencyId);
    const makeName = String(agency?.brand_name || '').trim();

    if (!makeName) {
      setAvailableModels([]);
      setModelSearch('');
      setModelTargets({});
      return;
    }

    setLoadingModels(true);
    try {
      const response = await vehicleCatalogApi.getModels(makeName, { allYears: true });
      const items = Array.isArray(response?.data?.items) ? response.data.items : [];
      const models = items
        .map((item) => ({
          name: String(item?.name || '').trim(),
          min_msrp: Number(item?.min_msrp || 0),
        }))
        .filter((item) => item.name)
        .sort((a, b) => a.name.localeCompare(b.name, 'es-MX'));
      setAvailableModels(models);
      setModelSearch('');
    } catch (error) {
      setAvailableModels([]);
      setModelSearch('');
      setModelTargets({});
      toast.error('No se pudo cargar el catálogo de modelos para esta marca');
    } finally {
      setLoadingModels(false);
    }
  }, [filters.agencies]);

  const loadAgencySellers = useCallback(async (agencyId) => {
    if (!agencyId) {
      setAvailableSellers([]);
      return;
    }
    setLoadingSellers(true);
    try {
      const response = await sellersApi.getAll({ agency_id: agencyId });
      const sellers = Array.isArray(response?.data) ? response.data : [];
      setAvailableSellers(sellers);
    } catch (error) {
      setAvailableSellers([]);
      toast.error('No se pudo cargar la lista de vendedores');
    } finally {
      setLoadingSellers(false);
    }
  }, []);

  useEffect(() => {
    if (!canCreateObjectives) return;
    loadAgencyModels(formData.agency_id);
  }, [canCreateObjectives, formData.agency_id, loadAgencyModels]);

  useEffect(() => {
    if (!canCreateObjectives || captureScope !== CAPTURE_SCOPE.SELLER) return;
    loadAgencySellers(formData.agency_id);
  }, [canCreateObjectives, captureScope, formData.agency_id, loadAgencySellers]);

  useEffect(() => {
    if (captureScope !== CAPTURE_SCOPE.SELLER) return;
    if (!formData.seller_id) return;
    const exists = availableSellers.some((seller) => seller.id === formData.seller_id);
    if (!exists) {
      setFormData((prev) => ({ ...prev, seller_id: '' }));
    }
  }, [captureScope, formData.seller_id, availableSellers]);

  const scopedObjectives = useMemo(() => (
    objectives.filter((objective) => {
      if (!formData.agency_id) return false;
      if (objective?.agency_id !== formData.agency_id) return false;
      if (Number(objective?.month) !== Number(formData.month)) return false;
      if (Number(objective?.year) !== Number(formData.year)) return false;
      if (captureScope === CAPTURE_SCOPE.SELLER) {
        if (!formData.seller_id) return false;
        return String(objective?.seller_id || '') === formData.seller_id;
      }
      return !objective?.seller_id;
    })
  ), [
    objectives,
    captureScope,
    formData.agency_id,
    formData.month,
    formData.year,
    formData.seller_id,
  ]);
  const canSuggestByHistory = (
    captureScope === CAPTURE_SCOPE.SELLER
    && Boolean(formData.agency_id)
    && Boolean(formData.seller_id)
  );
  const sellerOptions = availableSellers
    .filter((seller) => Boolean(seller?.id))
    .reduce((acc, seller) => {
      if (!acc.some((item) => item.id === seller.id)) acc.push(seller);
      return acc;
    }, []);

  useEffect(() => {
    if (!canCreateObjectives || captureMode !== CAPTURE_MODE.MODEL) return;
    if (!formData.agency_id || loadingModels || availableModels.length === 0) return;
    if (captureScope === CAPTURE_SCOPE.SELLER && !formData.seller_id) {
      setModelTargets({});
      return;
    }
    const existingByModel = new Map(
      scopedObjectives
        .filter((objective) => String(objective?.vehicle_line || '').trim())
        .map((objective) => [String(objective?.vehicle_line || '').trim().toLowerCase(), objective])
    );

    const nextTargets = {};
    availableModels.forEach((modelItem) => {
      const modelKey = modelItem.name.toLowerCase();
      const existingObjective = existingByModel.get(modelKey);
      nextTargets[modelItem.name] = existingObjective ? String(Math.max(0, Number(existingObjective.units_target || 0))) : '';
    });

    setModelTargets((prev) => {
      const prevKeys = Object.keys(prev);
      const nextKeys = Object.keys(nextTargets);
      if (prevKeys.length !== nextKeys.length) return nextTargets;
      for (const key of nextKeys) {
        if ((prev[key] || '') !== (nextTargets[key] || '')) {
          return nextTargets;
        }
      }
      return prev;
    });
  }, [
    canCreateObjectives,
    captureMode,
    captureScope,
    formData.agency_id,
    formData.seller_id,
    formData.month,
    formData.year,
    availableModels,
    loadingModels,
    scopedObjectives,
  ]);

  useEffect(() => {
    if (!canCreateObjectives || captureMode !== CAPTURE_MODE.TOTAL) return;
    if (!formData.agency_id) return;
    if (captureScope === CAPTURE_SCOPE.SELLER && !formData.seller_id) {
      setTotalUnitsInput('');
      setTotalRevenueInput('');
      return;
    }
    const totalObjective = scopedObjectives.find((objective) => !String(objective?.vehicle_line || '').trim());
    const nextUnits = totalObjective ? String(Math.max(0, Number(totalObjective.units_target || 0))) : '';
    const nextRevenue = totalObjective ? String(Math.max(0, Number(totalObjective.revenue_target || 0))) : '';
    setTotalUnitsInput((prev) => (prev === nextUnits ? prev : nextUnits));
    setTotalRevenueInput((prev) => (prev === nextRevenue ? prev : nextRevenue));
  }, [
    canCreateObjectives,
    captureMode,
    captureScope,
    formData.agency_id,
    formData.seller_id,
    scopedObjectives,
  ]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = {
        ...getFilterParams(),
        month: selectedMonth,
        year: selectedYear,
        include_seller_objectives: true,
      };
      
      const [objectivesRes, performanceRes] = await Promise.all([
        salesObjectivesApi.getAll(params),
        dashboardApi.getSellerPerformance({ ...params, month: selectedMonth, year: selectedYear })
      ]);
      
      setObjectives(objectivesRes.data);
      setSellerPerformance(performanceRes.data);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [getFilterParams, selectedMonth, selectedYear]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const saveObjectives = async (submitForApproval = true) => {
    const saveAsDraft = !submitForApproval;
    if (!formData.agency_id) {
      toast.error('Selecciona una agencia para capturar el objetivo');
      return;
    }
    if (captureScope === CAPTURE_SCOPE.SELLER && !formData.seller_id) {
      toast.error('Selecciona un vendedor para capturar objetivo por vendedor');
      return;
    }

    const targetSellerId = captureScope === CAPTURE_SCOPE.SELLER ? formData.seller_id : '';
    const scopedForSave = objectives.filter((objective) => {
      if (objective?.agency_id !== formData.agency_id) return false;
      if (Number(objective?.month) !== Number(formData.month)) return false;
      if (Number(objective?.year) !== Number(formData.year)) return false;
      if (targetSellerId) return String(objective?.seller_id || '') === targetSellerId;
      return !objective?.seller_id;
    });

    if (captureMode === CAPTURE_MODE.TOTAL) {
      const rawUnits = String(totalUnitsInput || '').trim();
      if (!/^\d+$/.test(rawUnits)) {
        toast.error('Meta unidades debe ser entero positivo (sin decimales)');
        return;
      }
      const unitsTarget = Number(rawUnits);
      if (unitsTarget <= 0) {
        toast.error('Meta unidades debe ser mayor a 0');
        return;
      }
      const revenueTarget = Number(totalRevenueInput);
      if (!Number.isFinite(revenueTarget) || revenueTarget < 0) {
        toast.error('Meta ingresos debe ser un número válido (>= 0)');
        return;
      }
      const existingTotalObjective = scopedForSave.find((objective) => !String(objective?.vehicle_line || '').trim());
      const payload = {
        agency_id: formData.agency_id,
        seller_id: targetSellerId || null,
        month: Number(formData.month),
        year: Number(formData.year),
        units_target: unitsTarget,
        revenue_target: revenueTarget,
        vehicle_line: null,
        save_as_draft: saveAsDraft,
      };

      try {
        if (existingTotalObjective?.id) {
          await salesObjectivesApi.update(existingTotalObjective.id, payload);
          toast.success(saveAsDraft ? 'Borrador total guardado' : 'Objetivo total aplicado');
        } else {
          await salesObjectivesApi.create(payload);
          toast.success(saveAsDraft ? 'Borrador total creado' : 'Objetivo total aplicado');
        }
        await fetchData();
      } catch (error) {
        const detail = error.response?.data?.detail;
        toast.error(typeof detail === 'string' ? detail : 'Error al guardar objetivo total');
      }
      return;
    }

    const rowsToSave = availableModels
      .map((modelItem) => {
        const rawUnits = String(modelTargets[modelItem.name] || '').trim();
        if (!rawUnits) return null;
        if (!/^\d+$/.test(rawUnits)) return { invalid: true, model: modelItem.name };
        const units = Number(rawUnits);
        if (units <= 0) return null;
        const minMsrp = Number(modelItem.min_msrp || 0);
        const revenueTarget = minMsrp > 0 ? Math.round(units * minMsrp) : 0;
        return { model: modelItem.name, unitsTarget: units, revenueTarget };
      })
      .filter(Boolean);

    const invalidRows = rowsToSave.filter((row) => row.invalid);
    if (invalidRows.length > 0) {
      toast.error(`Unidades inválidas en ${invalidRows.length} modelo(s). Usa enteros sin decimales.`);
      return;
    }

    const validRows = rowsToSave.filter((row) => !row.invalid);
    if (validRows.length === 0) {
      toast.error('Captura al menos una meta de unidades (> 0) por modelo');
      return;
    }

    const existingByModel = new Map(
      scopedForSave
        .filter((objective) => String(objective?.vehicle_line || '').trim())
        .map((objective) => [String(objective?.vehicle_line || '').trim().toLowerCase(), objective])
    );

    try {
      const saveResults = await Promise.allSettled(
        validRows.map(async (row) => {
          const payload = {
            agency_id: formData.agency_id,
            month: Number(formData.month),
            year: Number(formData.year),
            seller_id: targetSellerId || null,
            units_target: row.unitsTarget,
            revenue_target: row.revenueTarget,
            vehicle_line: row.model,
            save_as_draft: saveAsDraft,
          };
          const existingObjective = existingByModel.get(row.model.toLowerCase());
          if (existingObjective?.id) {
            await salesObjectivesApi.update(existingObjective.id, payload);
            return { mode: 'updated' };
          }
          await salesObjectivesApi.create(payload);
          return { mode: 'created' };
        })
      );

      const successful = saveResults.filter((result) => result.status === 'fulfilled');
      const failed = saveResults.length - successful.length;
      const createdCount = successful.filter((result) => result.value?.mode === 'created').length;
      const updatedCount = successful.filter((result) => result.value?.mode === 'updated').length;
      const failedRows = [];
      saveResults.forEach((result, index) => {
        if (result.status === 'rejected') {
          const row = validRows[index];
          const detail = result.reason?.response?.data?.detail || result.reason?.message || 'Error desconocido';
          failedRows.push({ model: row?.model || 'Modelo', detail: String(detail) });
        }
      });

      if (successful.length > 0) {
        toast.success(
          saveAsDraft
            ? `Borrador guardado: ${successful.length} (${createdCount} nuevos, ${updatedCount} actualizados)`
            : `Objetivos aplicados: ${successful.length} (${createdCount} nuevos, ${updatedCount} actualizados)`
        );
      }
      if (failed > 0) {
        const firstFailure = failedRows[0];
        toast.error(
          firstFailure
            ? `No se pudieron guardar ${failed} modelo(s). ${firstFailure.model}: ${firstFailure.detail}`
            : `No se pudieron guardar ${failed} modelo(s). Revisa permisos o datos.`
        );
      }

      if (failed === 0) setModelSearch('');
      await fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al crear objetivo');
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    await saveObjectives(true);
  };

  const handleSaveDraft = async () => {
    await saveObjectives(false);
  };

  const handleSuggestFromHistory = async () => {
    if (!formData.agency_id || !formData.seller_id) {
      toast.error('Selecciona agencia y vendedor para calcular sugerencias');
      return;
    }

    setLoadingSuggestion(true);
    try {
      const response = await salesObjectivesApi.suggest({
        agency_id: formData.agency_id,
        seller_id: formData.seller_id,
        month: Number(formData.month),
        year: Number(formData.year),
        lookback_months: 6,
      });
      const payload = response?.data || {};
      const items = Array.isArray(payload?.items) ? payload.items : [];
      const totals = payload?.totals || {};
      const baseline = payload?.baseline || {};

      if (captureMode === CAPTURE_MODE.TOTAL) {
        setTotalUnitsInput(String(Math.max(0, Number(totals?.suggested_units || 0))));
        setTotalRevenueInput(String(Math.max(0, Math.round(Number(totals?.suggested_revenue || 0)))));
        setLastSuggestionMeta({
          totalUnits: Number(totals?.suggested_units || 0),
          previousYearUnits: Number(baseline?.previous_year_same_month_units || 0),
          recentAvgUnits: Number(baseline?.recent_avg_units || 0),
          rowsApplied: 1,
        });
        toast.success('Objetivo total sugerido cargado');
        return;
      }

      const canonicalByLower = new Map(
        availableModels.map((modelItem) => [String(modelItem.name || '').trim().toLowerCase(), modelItem.name])
      );
      const nextTargets = {};
      availableModels.forEach((modelItem) => { nextTargets[modelItem.name] = ''; });

      let applied = 0;
      items.forEach((item) => {
        const key = String(item?.model || '').trim().toLowerCase();
        if (!key) return;
        const canonicalName = canonicalByLower.get(key);
        if (!canonicalName) return;
        const suggestedUnits = Math.max(0, Number(item?.suggested_units || 0));
        if (suggestedUnits <= 0) return;
        nextTargets[canonicalName] = String(Math.round(suggestedUnits));
        applied += 1;
      });
      setModelTargets(nextTargets);
      setLastSuggestionMeta({
        totalUnits: Number(totals?.suggested_units || 0),
        previousYearUnits: Number(baseline?.previous_year_same_month_units || 0),
        recentAvgUnits: Number(baseline?.recent_avg_units || 0),
        rowsApplied: applied,
      });
      if (applied > 0) {
        toast.success(`Sugerencias aplicadas en ${applied} modelos`);
      } else {
        toast.error('No hubo coincidencias de modelos con el catálogo de esta agencia');
      }
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo calcular sugerencias históricas');
    } finally {
      setLoadingSuggestion(false);
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0
    }).format(value);
  };

  const formatUnits = (value) => {
    return new Intl.NumberFormat('es-MX', {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(Number(value || 0));
  };

  const getObjectiveStatus = (objective) => {
    const status = String(objective?.approval_status || '').toLowerCase();
    if (
      status === OBJECTIVE_DRAFT
      || status === OBJECTIVE_PENDING
      || status === OBJECTIVE_REJECTED
      || status === OBJECTIVE_APPROVED
    ) {
      return status;
    }
    // Backward compatibility for existing records created before approval flow.
    return OBJECTIVE_APPROVED;
  };

  const getObjectiveStatusBadgeClass = (status) => {
    if (status === OBJECTIVE_DRAFT) return 'border-[#6B7280] text-[#4B5563]';
    if (status === OBJECTIVE_APPROVED) return 'border-[#2A9D8F] text-[#2A9D8F]';
    if (status === OBJECTIVE_REJECTED) return 'border-[#E63946] text-[#E63946]';
    return 'border-[#E9C46A] text-[#8A6D1A]';
  };

  const getSellerStatus = (row) => {
    if (!row || Number(row.units_target || 0) <= 0) {
      return { label: 'Sin objetivo', className: 'border-[#6B7280] text-[#4B5563]' };
    }
    const pct = Number(row.units_compliance || 0);
    if (pct >= 100) return { label: 'En meta', className: 'border-[#2A9D8F] text-[#2A9D8F]' };
    if (pct >= 80) return { label: 'En riesgo', className: 'border-[#E9C46A] text-[#8A6D1A]' };
    return { label: 'Bajo meta', className: 'border-[#E63946] text-[#E63946]' };
  };

  const sellerChartData = sellerPerformance.map((s) => ({
    name: s.seller_name?.split(' ')[0] || 'Unknown',
    units: Math.round(Number(s.units || 0)),
    commission: s.commission
  }));

  const sellerMonthlySummary = useMemo(() => {
    const objectivesBySeller = new Map();
    objectives.forEach((objective) => {
      const sellerId = String(objective?.seller_id || '').trim();
      if (!sellerId) return;

      const current = objectivesBySeller.get(sellerId) || {
        seller_id: sellerId,
        seller_name: objective?.seller_name || 'Vendedor',
        total_target: null,
        model_units_target: 0,
        model_revenue_target: 0,
      };

      const hasVehicleLine = Boolean(String(objective?.vehicle_line || '').trim());
      if (!hasVehicleLine) {
        current.total_target = {
          units_target: Number(objective?.units_target || 0),
          revenue_target: Number(objective?.revenue_target || 0),
        };
      } else {
        current.model_units_target += Number(objective?.units_target || 0);
        current.model_revenue_target += Number(objective?.revenue_target || 0);
      }

      if (!current.seller_name && objective?.seller_name) {
        current.seller_name = objective.seller_name;
      }
      objectivesBySeller.set(sellerId, current);
    });

    const performanceBySeller = new Map();
    sellerPerformance.forEach((seller) => {
      const sellerId = String(seller?.seller_id || '').trim();
      if (!sellerId) return;
      performanceBySeller.set(sellerId, {
        seller_id: sellerId,
        seller_name: seller?.seller_name || 'Vendedor',
        units: Number(seller?.units || 0),
        revenue: Number(seller?.revenue || 0),
        commission: Number(seller?.commission || 0),
      });
    });

    const sellerIds = new Set([
      ...objectivesBySeller.keys(),
      ...performanceBySeller.keys(),
    ]);

    const rows = [];
    sellerIds.forEach((sellerId) => {
      const objectiveData = objectivesBySeller.get(sellerId);
      const perfData = performanceBySeller.get(sellerId);
      const unitsTarget = objectiveData?.total_target
        ? Number(objectiveData.total_target.units_target || 0)
        : Number(objectiveData?.model_units_target || 0);
      const revenueTarget = objectiveData?.total_target
        ? Number(objectiveData.total_target.revenue_target || 0)
        : Number(objectiveData?.model_revenue_target || 0);

      const unitsSold = Number(perfData?.units || 0);
      const revenueSold = Number(perfData?.revenue || 0);
      const commission = Number(perfData?.commission || 0);

      const unitsCompliance = unitsTarget > 0 ? (unitsSold / unitsTarget) * 100 : null;
      const revenueCompliance = revenueTarget > 0 ? (revenueSold / revenueTarget) * 100 : null;
      const avgTicket = unitsSold > 0 ? revenueSold / unitsSold : 0;

      rows.push({
        seller_id: sellerId,
        seller_name: perfData?.seller_name || objectiveData?.seller_name || 'Vendedor',
        units_sold: unitsSold,
        units_target: unitsTarget,
        units_compliance: unitsCompliance,
        revenue_sold: revenueSold,
        revenue_target: revenueTarget,
        revenue_compliance: revenueCompliance,
        avg_ticket: avgTicket,
        commission,
      });
    });

    return rows.sort((a, b) => {
      if (b.units_sold !== a.units_sold) return b.units_sold - a.units_sold;
      return a.seller_name.localeCompare(b.seller_name, 'es-MX');
    });
  }, [objectives, sellerPerformance]);

  // Objetivos operativos visibles en el periodo (agencia y/o vendedor).
  const operationalObjectives = objectives;
  const brandsWithObjectivesCount = new Set(
    operationalObjectives
      .map((objective) => objective?.brand_id)
      .filter(Boolean)
  ).size;

  // Calculate totals
  const totalUnitsTarget = objectives.reduce((sum, o) => sum + (o.units_target || 0), 0);
  const totalUnitsSold = objectives.reduce((sum, o) => sum + (o.units_sold || 0), 0);
  const totalRevenueTarget = objectives.reduce((sum, o) => sum + (o.revenue_target || 0), 0);
  const totalRevenueAchieved = objectives.reduce((sum, o) => sum + (o.revenue_achieved || 0), 0);

  // Live preview totals while editing (without waiting for save)
  const scopedUnitsTarget = scopedObjectives.reduce((sum, o) => sum + Number(o?.units_target || 0), 0);
  const scopedRevenueTarget = scopedObjectives.reduce((sum, o) => sum + Number(o?.revenue_target || 0), 0);

  const draftScopeTargets = useMemo(() => {
    if (!canCreateObjectives) return null;
    if (!formData.agency_id) return null;
    if (captureScope === CAPTURE_SCOPE.SELLER && !formData.seller_id) return null;

    if (captureMode === CAPTURE_MODE.MODEL) {
      const minMsrpByModel = new Map(
        availableModels.map((modelItem) => [modelItem.name, Number(modelItem.min_msrp || 0)])
      );
      let unitsTarget = 0;
      let revenueTarget = 0;
      let hasAnyValue = false;

      Object.entries(modelTargets).forEach(([modelName, rawValue]) => {
        const digitsOnly = String(rawValue || '').replace(/[^\d]/g, '').trim();
        if (!digitsOnly) return;
        const units = Number(digitsOnly);
        if (!Number.isFinite(units) || units <= 0) return;
        hasAnyValue = true;
        unitsTarget += units;
        const minMsrp = Number(minMsrpByModel.get(modelName) || 0);
        if (minMsrp > 0) revenueTarget += units * minMsrp;
      });

      if (!hasAnyValue) return { unitsTarget: 0, revenueTarget: 0 };
      return { unitsTarget, revenueTarget };
    }

    const unitsRaw = String(totalUnitsInput || '').replace(/[^\d]/g, '').trim();
    const revenueRaw = String(totalRevenueInput || '').replace(/[^\d]/g, '').trim();
    const hasAnyValue = unitsRaw.length > 0 || revenueRaw.length > 0;
    if (!hasAnyValue) return { unitsTarget: 0, revenueTarget: 0 };

    return {
      unitsTarget: Number(unitsRaw || 0),
      revenueTarget: Number(revenueRaw || 0),
    };
  }, [
    canCreateObjectives,
    formData.agency_id,
    formData.seller_id,
    captureScope,
    captureMode,
    availableModels,
    modelTargets,
    totalUnitsInput,
    totalRevenueInput,
  ]);

  const previewUnitsTarget = draftScopeTargets
    ? Math.max(0, totalUnitsTarget - scopedUnitsTarget + Number(draftScopeTargets.unitsTarget || 0))
    : totalUnitsTarget;
  const previewRevenueTarget = draftScopeTargets
    ? Math.max(0, totalRevenueTarget - scopedRevenueTarget + Number(draftScopeTargets.revenueTarget || 0))
    : totalRevenueTarget;

  return (
    <div className="space-y-6" data-testid="objectives-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Objetivos de Ventas
          </h1>
          <p className="text-muted-foreground">
            Gestiona objetivos por agencia, vendedor, modelo o totales
          </p>
        </div>
        <div className="flex gap-2">
          <select
            value={selectedMonth}
            onChange={(e) => setSelectedMonth(parseInt(e.target.value, 10))}
            className="h-9 w-[140px] rounded-md border border-input bg-transparent px-3 py-2 text-sm"
            data-testid="select-month"
          >
            {MONTHS.map((m) => (
              <option key={m.value} value={m.value}>{m.label}</option>
            ))}
          </select>
          <select
            value={selectedYear}
            onChange={(e) => setSelectedYear(parseInt(e.target.value, 10))}
            className="h-9 w-[100px] rounded-md border border-input bg-transparent px-3 py-2 text-sm"
            data-testid="select-year"
          >
            {[2024, 2025, 2026].map((y) => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Hierarchical Filters */}
      <HierarchicalFilters filters={filters} includeSellers={false} />

      {/* Summary Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="border-border/40">
          <CardContent className="p-4">
            <div className="text-xs font-semibold tracking-widest uppercase text-muted-foreground mb-1">
              Meta Unidades
            </div>
            <div className="text-2xl font-bold">{formatUnits(previewUnitsTarget)}</div>
            <Progress 
              value={previewUnitsTarget > 0 ? (totalUnitsSold / previewUnitsTarget * 100) : 0} 
              className="h-2 mt-2" 
            />
            <div className="text-sm text-muted-foreground mt-1">
              {formatUnits(totalUnitsSold)} vendidas ({previewUnitsTarget > 0 ? ((totalUnitsSold / previewUnitsTarget) * 100).toFixed(1) : 0}%)
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/40">
          <CardContent className="p-4">
            <div className="text-xs font-semibold tracking-widest uppercase text-muted-foreground mb-1">
              Meta Ingresos
            </div>
            <div className="text-2xl font-bold">{formatCurrency(previewRevenueTarget)}</div>
            <Progress 
              value={previewRevenueTarget > 0 ? (totalRevenueAchieved / previewRevenueTarget * 100) : 0} 
              className="h-2 mt-2" 
            />
            <div className="text-sm text-muted-foreground mt-1">
              {formatCurrency(totalRevenueAchieved)} ({previewRevenueTarget > 0 ? ((totalRevenueAchieved / previewRevenueTarget) * 100).toFixed(1) : 0}%)
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/40">
          <CardContent className="p-4">
            <div className="text-xs font-semibold tracking-widest uppercase text-muted-foreground mb-1">
              Objetivos Configurados
            </div>
            <div className="text-2xl font-bold">{operationalObjectives.length}</div>
            <div className="text-sm text-muted-foreground mt-1">
              configurados
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/40">
          <CardContent className="p-4">
            <div className="text-xs font-semibold tracking-widest uppercase text-muted-foreground mb-1">
              Marcas con Objetivo
            </div>
            <div className="text-2xl font-bold">{brandsWithObjectivesCount}</div>
            <div className="text-sm text-muted-foreground mt-1">
              activas en el periodo
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Objectives Grid */}
      <div className="space-y-4">
        <h2 className="text-lg font-semibold flex items-center gap-2" style={{ fontFamily: 'Cabinet Grotesk' }}>
          <Storefront size={20} weight="duotone" className="text-[#002FA7]" />
          Objetivos por Agencia y Vendedor
        </h2>
        {canCreateObjectives && (
          <Card className="border-border/40">
            <CardHeader className="pb-3">
              <CardTitle className="text-base" style={{ fontFamily: 'Cabinet Grotesk' }}>
                Captura En Lienzo (Sin Popup)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="grid md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="capture_scope">Ámbito</Label>
                    <select
                      id="capture_scope"
                      value={captureScope}
                      onChange={(e) => setCaptureScope(e.target.value)}
                      className="h-9 w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm"
                      data-testid="objective-scope-select"
                    >
                      <option value={CAPTURE_SCOPE.AGENCY}>Agencia</option>
                      <option value={CAPTURE_SCOPE.SELLER}>Vendedor</option>
                    </select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="capture_mode">Tipo de Captura</Label>
                    <select
                      id="capture_mode"
                      value={captureMode}
                      onChange={(e) => setCaptureMode(e.target.value)}
                      className="h-9 w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm"
                      data-testid="objective-mode-select"
                    >
                      <option value={CAPTURE_MODE.MODEL}>Por Modelo</option>
                      <option value={CAPTURE_MODE.TOTAL}>Totales</option>
                    </select>
                  </div>
                </div>

                <div className="grid md:grid-cols-4 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="agency_id">Agencia</Label>
                    <select
                      id="agency_id"
                      value={formData.agency_id}
                      onChange={(e) => {
                        const value = e.target.value;
                        setModelSearch('');
                        setModelTargets({});
                        setTotalUnitsInput('');
                        setTotalRevenueInput('');
                        setFormData({ ...formData, agency_id: value, seller_id: '' });
                      }}
                      disabled={shouldLockAgencySelector}
                      className="h-9 w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm disabled:opacity-50"
                      data-testid="objective-agency-select"
                    >
                      <option value="">Seleccionar agencia</option>
                      {filters.filteredAgencies.map((agency) => (
                        <option key={agency.id} value={agency.id}>
                          {agency.name}
                        </option>
                      ))}
                    </select>
                  </div>
                  {captureScope === CAPTURE_SCOPE.SELLER && (
                    <div className="space-y-2">
                      <Label htmlFor="seller_id">Vendedor</Label>
                      <select
                        id="seller_id"
                        value={formData.seller_id}
                        onChange={(e) => setFormData({ ...formData, seller_id: e.target.value })}
                        disabled={!formData.agency_id || loadingSellers}
                        className="h-9 w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm disabled:opacity-50"
                        data-testid="objective-seller-select"
                      >
                        <option value="">
                          {loadingSellers ? 'Cargando vendedores...' : 'Seleccionar vendedor'}
                        </option>
                        {sellerOptions.map((seller) => (
                          <option key={seller.id} value={seller.id}>
                            {seller.name}
                          </option>
                        ))}
                      </select>
                    </div>
                  )}
                  <div className="space-y-2">
                    <Label htmlFor="month">Mes</Label>
                    <select
                      id="month"
                      value={formData.month}
                      onChange={(e) => setFormData({ ...formData, month: parseInt(e.target.value, 10) })}
                      className="h-9 w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm"
                      data-testid="objective-month-select"
                    >
                      {MONTHS.map((m) => (
                        <option key={m.value} value={m.value}>
                          {m.label}
                        </option>
                      ))}
                    </select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="year">Año</Label>
                    <select
                      id="year"
                      value={formData.year}
                      onChange={(e) => setFormData({ ...formData, year: parseInt(e.target.value, 10) })}
                      className="h-9 w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm"
                      data-testid="objective-year-select"
                    >
                      {[2024, 2025, 2026].map((y) => (
                        <option key={y} value={y}>
                          {y}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>

                {captureMode === CAPTURE_MODE.MODEL ? (
                  <div className="space-y-2">
                    <Label htmlFor="vehicle_line">Objetivo por Modelo (unidades)</Label>
                    <Input
                      id="objective-model-search"
                      value={modelSearch}
                      onChange={(e) => setModelSearch(e.target.value)}
                      placeholder="Buscar modelo..."
                      disabled={
                        !formData.agency_id
                        || loadingModels
                        || (captureScope === CAPTURE_SCOPE.SELLER && !formData.seller_id)
                      }
                    />
                    <div className="rounded-md border border-border/60 overflow-x-auto" data-testid="objective-line-input">
                      {!formData.agency_id ? (
                        <p className="text-xs text-muted-foreground p-3">Selecciona una agencia para ver modelos.</p>
                      ) : captureScope === CAPTURE_SCOPE.SELLER && !formData.seller_id ? (
                        <p className="text-xs text-muted-foreground p-3">Selecciona un vendedor para capturar objetivos por modelo.</p>
                      ) : loadingModels ? (
                        <p className="text-xs text-muted-foreground p-3">Cargando modelos...</p>
                      ) : (
                        <table className="w-full text-sm">
                          <thead className="bg-muted/40 sticky top-0">
                            <tr>
                              <th className="text-left px-3 py-2 font-semibold">Modelo</th>
                              <th className="text-right px-3 py-2 font-semibold">Min MSRP</th>
                              <th className="text-right px-3 py-2 font-semibold">Meta Unidades</th>
                              <th className="text-right px-3 py-2 font-semibold">Meta Ingresos (auto)</th>
                            </tr>
                          </thead>
                          <tbody>
                            {availableModels
                              .filter((modelItem) => modelItem.name.toLowerCase().includes(modelSearch.toLowerCase()))
                              .map((modelItem) => {
                                const unitsRaw = String(modelTargets[modelItem.name] || '');
                                const units = Number(unitsRaw || 0);
                                const minMsrp = Number(modelItem.min_msrp || 0);
                                const autoRevenue = units > 0 && minMsrp > 0 ? units * minMsrp : 0;
                                return (
                                  <tr key={modelItem.name} className="border-t border-border/40">
                                    <td className="px-3 py-2">{modelItem.name}</td>
                                    <td className="px-3 py-2 text-right text-muted-foreground">
                                      {minMsrp > 0 ? formatCurrency(minMsrp) : 'N/D'}
                                    </td>
                                    <td className="px-3 py-2 text-right">
                                      <Input
                                        type="number"
                                        min={0}
                                        step={1}
                                        inputMode="numeric"
                                        value={unitsRaw}
                                        onChange={(e) => {
                                          const integerOnly = String(e.target.value || '').replace(/[^\d]/g, '');
                                          setModelTargets((prev) => ({ ...prev, [modelItem.name]: integerOnly }));
                                        }}
                                        placeholder="0"
                                        className="w-24 ml-auto text-right"
                                        data-testid={`objective-units-input-${modelItem.name}`}
                                      />
                                    </td>
                                    <td className="px-3 py-2 text-right font-medium">
                                      {formatCurrency(autoRevenue)}
                                    </td>
                                  </tr>
                                );
                              })}
                          </tbody>
                        </table>
                      )}
                    </div>
                    {!loadingModels && formData.agency_id && availableModels.length === 0 && (
                      <p className="text-xs text-muted-foreground">
                        No se encontraron modelos para esta marca en el catálogo.
                      </p>
                    )}
                    <p className="text-xs text-muted-foreground">
                      Solo se guardan filas con unidades mayores a 0. La meta ingresos se calcula en automático con MSRP mínimo por modelo.
                    </p>
                  </div>
                ) : (
                  <div className="grid md:grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="total_units">Meta Unidades (total)</Label>
                      <Input
                        id="total_units"
                        type="number"
                        min={0}
                        step={1}
                        inputMode="numeric"
                        value={totalUnitsInput}
                        onChange={(e) => setTotalUnitsInput(String(e.target.value || '').replace(/[^\d]/g, ''))}
                        placeholder="Ej: 80"
                        data-testid="objective-total-units-input"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="total_revenue">Meta Ingresos (total)</Label>
                      <Input
                        id="total_revenue"
                        type="number"
                        min={0}
                        step={1}
                        inputMode="numeric"
                        value={totalRevenueInput}
                        onChange={(e) => setTotalRevenueInput(String(e.target.value || '').replace(/[^\d]/g, ''))}
                        placeholder="Ej: 25000000"
                        data-testid="objective-total-revenue-input"
                      />
                    </div>
                    <p className="text-xs text-muted-foreground md:col-span-2">
                      En modo Totales se guarda un solo objetivo agregado (sin modelo) para el alcance seleccionado.
                    </p>
                  </div>
                )}

                <div className="flex items-center justify-between gap-3">
                  <div className="flex-1">
                    {canSuggestByHistory && (
                      <Button
                        type="button"
                        variant="outline"
                        onClick={handleSuggestFromHistory}
                        disabled={loadingSuggestion || (captureMode === CAPTURE_MODE.MODEL && loadingModels)}
                        data-testid="suggest-objective-btn"
                      >
                        {loadingSuggestion ? 'Calculando sugerencia...' : 'Sugerir por histórico'}
                      </Button>
                    )}
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={handleSaveDraft}
                    data-testid="save-objective-draft-btn"
                  >
                    Guardar
                  </Button>
                  <Button
                    type="submit"
                    className="bg-[#002FA7] hover:bg-[#002FA7]/90"
                    data-testid="save-objective-btn"
                  >
                    {objectiveSubmitLabel}
                  </Button>
                </div>
                {lastSuggestionMeta && canSuggestByHistory && (
                  <p className="text-xs text-muted-foreground">
                    Sugerencia aplicada. Año pasado mismo mes: {formatUnits(lastSuggestionMeta.previousYearUnits)} u ·
                    promedio reciente: {Number(lastSuggestionMeta.recentAvgUnits || 0).toFixed(1)} u/mes ·
                    objetivo sugerido: {formatUnits(lastSuggestionMeta.totalUnits)} u.
                  </p>
                )}
              </form>
            </CardContent>
          </Card>
        )}
        {loading ? (
          <Card className="border-border/40">
            <CardContent className="p-4">
              <Skeleton className="h-64 w-full" />
            </CardContent>
          </Card>
        ) : operationalObjectives.length === 0 ? (
          <Card className="border-border/40">
            <CardContent className="p-12 text-center">
              <Target size={48} className="mx-auto text-muted-foreground mb-4" />
              <p className="text-muted-foreground">No hay objetivos para este período</p>
            </CardContent>
          </Card>
        ) : (
          <Card className="border-border/40">
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <CardTitle className="text-base" style={{ fontFamily: 'Cabinet Grotesk' }}>
                Listado Compacto
              </CardTitle>
              <div className="text-sm text-muted-foreground">
                {operationalObjectives.length} objetivos
              </div>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto rounded-md border border-border/40">
                <table className="w-full text-sm">
                  <thead className="bg-muted/40 sticky top-0">
                    <tr>
                      <th className="text-left px-3 py-2 font-semibold">Agencia</th>
                      <th className="text-left px-3 py-2 font-semibold">Vendedor</th>
                      <th className="text-left px-3 py-2 font-semibold">Objetivo</th>
                      <th className="text-left px-3 py-2 font-semibold">Estatus</th>
                      <th className="text-right px-3 py-2 font-semibold">Unidades</th>
                      <th className="text-right px-3 py-2 font-semibold">Meta Ingresos</th>
                      <th className="text-right px-3 py-2 font-semibold">Ingresos</th>
                      <th className="text-right px-3 py-2 font-semibold">Acciones</th>
                    </tr>
                  </thead>
                  <tbody>
                    {operationalObjectives.map((objective) => {
                      const objectiveStatus = getObjectiveStatus(objective);
                      const objectiveLabel = String(objective?.vehicle_line || '').trim() || 'TOTAL';
                      return (
                        <tr key={objective.id} className="border-t border-border/40" data-testid={`objective-row-${objective.id}`}>
                          <td className="px-3 py-2">
                            <div className="font-medium">{objective.agency_name}</div>
                            <div className="text-xs text-muted-foreground">{objective.brand_name}</div>
                          </td>
                          <td className="px-3 py-2">{objective.seller_name || 'Equipo agencia'}</td>
                          <td className="px-3 py-2">{objectiveLabel}</td>
                          <td className="px-3 py-2">
                            <Badge variant="outline" className={getObjectiveStatusBadgeClass(objectiveStatus)}>
                              {OBJECTIVE_STATUS_LABELS[objectiveStatus]}
                            </Badge>
                            {objectiveStatus === OBJECTIVE_REJECTED && objective.approval_comment && (
                              <div className="text-xs text-[#E63946] mt-1">{objective.approval_comment}</div>
                            )}
                          </td>
                          <td className="px-3 py-2 text-right font-medium">
                            {formatUnits(objective.units_sold)} / {formatUnits(objective.units_target)}
                          </td>
                          <td className="px-3 py-2 text-right">{formatCurrency(objective.revenue_target)}</td>
                          <td className="px-3 py-2 text-right">{formatCurrency(objective.revenue_achieved)}</td>
                          <td className="px-3 py-2 text-right">
                            {objectiveStatus === OBJECTIVE_DRAFT ? (
                              <span className="text-xs text-[#4B5563]">Borrador</span>
                            ) : (
                              <span className="text-xs text-[#2A9D8F]">
                                Aplicado{objective.updated_at ? `: ${new Date(objective.updated_at).toLocaleDateString('es-MX')}` : ''}
                              </span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        )}
      </div>

      {/* Seller Performance */}
      <div className="grid lg:grid-cols-2 gap-6">
        <Card className="border-border/40" data-testid="seller-performance-chart">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Cabinet Grotesk' }}>
              <Users size={20} weight="duotone" className="text-[#002FA7]" />
              Desempeño de Vendedores
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-64 w-full" />
            ) : sellerPerformance.length === 0 ? (
              <div className="h-64 flex items-center justify-center text-muted-foreground">
                No hay datos de vendedores
              </div>
            ) : (
              <div className="h-64">
                <SafeChart resetKey={`${selectedMonth}-${selectedYear}-${sellerChartData.length}`}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={sellerChartData} layout="vertical">
                      <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                      <XAxis
                        type="number"
                        allowDecimals={false}
                        tick={{ fontSize: 12 }}
                        stroke="hsl(var(--muted-foreground))"
                      />
                      <YAxis dataKey="name" type="category" tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" width={80} />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: 'hsl(var(--card))',
                          border: '1px solid hsl(var(--border))',
                          borderRadius: '6px'
                        }}
                        formatter={(value, name) => [
                          name === 'Unidades' ? formatUnits(value) : value,
                          name
                        ]}
                      />
                      <Bar dataKey="units" fill="#002FA7" name="Unidades" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </SafeChart>
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="border-border/40" data-testid="seller-performance-table">
          <CardHeader>
            <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
              Resumen Mensual por Vendedor
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="space-y-3">
                {[1, 2, 3, 4].map((i) => (
                  <Skeleton key={i} className="h-12 w-full" />
                ))}
              </div>
            ) : sellerMonthlySummary.length === 0 ? (
              <div className="py-12 text-center text-muted-foreground">
                No hay datos mensuales por vendedor
              </div>
            ) : (
              <div className="rounded-md border border-border/40 overflow-auto">
                <table className="w-full text-sm">
                  <thead className="bg-muted/40 sticky top-0">
                    <tr>
                      <th className="text-left px-3 py-2 font-semibold">Vendedor</th>
                      <th className="text-right px-3 py-2 font-semibold">Unidades</th>
                      <th className="text-right px-3 py-2 font-semibold">Meta U</th>
                      <th className="text-right px-3 py-2 font-semibold">Cumpl. U</th>
                      <th className="text-right px-3 py-2 font-semibold">Ingresos</th>
                      <th className="text-right px-3 py-2 font-semibold">Meta Ing.</th>
                      <th className="text-right px-3 py-2 font-semibold">Cumpl. Ing.</th>
                      <th className="text-right px-3 py-2 font-semibold">Ticket Prom.</th>
                      <th className="text-right px-3 py-2 font-semibold">Comisión</th>
                      <th className="text-right px-3 py-2 font-semibold">Estado</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sellerMonthlySummary.map((row) => {
                      const sellerStatus = getSellerStatus(row);
                      return (
                        <tr key={row.seller_id} className="border-t border-border/40">
                          <td className="px-3 py-2 font-medium">{row.seller_name}</td>
                          <td className="px-3 py-2 text-right">{formatUnits(row.units_sold)}</td>
                          <td className="px-3 py-2 text-right">{formatUnits(row.units_target)}</td>
                          <td className="px-3 py-2 text-right">
                            {row.units_compliance === null ? 'N/D' : `${row.units_compliance.toFixed(1)}%`}
                          </td>
                          <td className="px-3 py-2 text-right">{formatCurrency(row.revenue_sold)}</td>
                          <td className="px-3 py-2 text-right">{formatCurrency(row.revenue_target)}</td>
                          <td className="px-3 py-2 text-right">
                            {row.revenue_compliance === null ? 'N/D' : `${row.revenue_compliance.toFixed(1)}%`}
                          </td>
                          <td className="px-3 py-2 text-right">{formatCurrency(row.avg_ticket)}</td>
                          <td className="px-3 py-2 text-right">{formatCurrency(row.commission)}</td>
                          <td className="px-3 py-2 text-right">
                            <Badge variant="outline" className={sellerStatus.className}>
                              {sellerStatus.label}
                            </Badge>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

    </div>
  );
}
