import { useState, useEffect, useMemo, useCallback } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { priceBulletinsApi, vehicleCatalogApi } from '../lib/api';
import { useHierarchicalFilters, HierarchicalFilters } from '../components/HierarchicalFilters';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Skeleton } from '../components/ui/skeleton';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '../components/ui/table';
import { toast } from 'sonner';

const PRICE_BULLETIN_EDITOR_ROLES = [
  'app_admin',
  'group_admin',
  'group_finance_manager',
  'brand_admin',
  'agency_general_manager',
  'agency_sales_manager',
  'agency_admin',
  'agency_commercial_manager',
];

const todayDateString = () => new Date().toISOString().slice(0, 10);
const normalizeText = (value) => String(value || '').trim();
const makeBulletinKey = (model, version = '') =>
  `${normalizeText(model).toLowerCase()}::${normalizeText(version).toLowerCase()}`;
const makeTestIdSuffix = (model, version = '') =>
  `${normalizeText(model)}-${normalizeText(version || 'base')}`
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
const toNonNegativeNumber = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.max(0, parsed);
};
const roundMoney = (value) => Math.round((toNonNegativeNumber(value) + Number.EPSILON) * 100) / 100;
const clampPercentage = (value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  return Math.min(100, Math.max(0, parsed));
};
const computeFromPrice = (catalogPrice, bulletinPrice, dealerSharePct) => {
  const catalog = toNonNegativeNumber(catalogPrice);
  const cappedPrice = catalog > 0 ? Math.min(toNonNegativeNumber(bulletinPrice), catalog) : toNonNegativeNumber(bulletinPrice);
  const dealerPct = clampPercentage(dealerSharePct);
  const totalIncentive = Math.max(catalog - cappedPrice, 0);
  const dealerIncentive = roundMoney(totalIncentive * (dealerPct / 100));
  const brandIncentive = roundMoney(Math.max(totalIncentive - dealerIncentive, 0));
  return {
    msrp: roundMoney(cappedPrice),
    brand_bonus_amount: brandIncentive,
    dealer_bonus_amount: dealerIncentive,
    dealer_share_percentage: dealerPct,
  };
};
const computeFromBrandIncentive = (catalogPrice, brandIncentive, dealerSharePct) => {
  const catalog = toNonNegativeNumber(catalogPrice);
  const dealerPct = clampPercentage(dealerSharePct);
  const brandSharePct = Math.max(0, 100 - dealerPct);
  if (brandSharePct <= 0) {
    return computeFromPrice(catalog, catalog, dealerPct);
  }
  const desiredBrand = roundMoney(brandIncentive);
  const totalIncentiveRaw = desiredBrand / (brandSharePct / 100);
  const totalIncentive = Math.min(roundMoney(totalIncentiveRaw), catalog);
  const bulletinPrice = Math.max(roundMoney(catalog - totalIncentive), 0);
  return computeFromPrice(catalog, bulletinPrice, dealerPct);
};

export default function PricesPage() {
  const { user } = useAuth();
  const filters = useHierarchicalFilters();
  const {
    groups,
    brands,
    agencies,
    selectedGroup,
    selectedBrand,
    selectedAgency,
  } = filters;

  const canEdit = PRICE_BULLETIN_EDITOR_ROLES.includes(user?.role);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [rows, setRows] = useState([]);
  const [globalDealerSharePct, setGlobalDealerSharePct] = useState(0);
  const [meta, setMeta] = useState({
    bulletin_name: '',
    effective_from: todayDateString(),
    effective_to: '',
    notes: '',
  });

  const resolvedAgencyId = useMemo(() => {
    if (selectedAgency !== 'all') return selectedAgency;
    if (user?.agency_id) return user.agency_id;
    if (agencies.length === 1) return agencies[0].id;
    return '';
  }, [selectedAgency, user?.agency_id, agencies]);

  const resolvedBrandId = useMemo(() => {
    if (selectedBrand !== 'all') return selectedBrand;
    if (resolvedAgencyId) {
      return agencies.find((item) => item.id === resolvedAgencyId)?.brand_id || '';
    }
    if (user?.brand_id) return user.brand_id;
    if (brands.length === 1) return brands[0].id;
    return '';
  }, [selectedBrand, resolvedAgencyId, agencies, user?.brand_id, brands]);

  const resolvedGroupId = useMemo(() => {
    if (selectedGroup !== 'all') return selectedGroup;
    if (resolvedBrandId) {
      return brands.find((item) => item.id === resolvedBrandId)?.group_id || '';
    }
    if (resolvedAgencyId) {
      return agencies.find((item) => item.id === resolvedAgencyId)?.group_id || '';
    }
    if (user?.group_id) return user.group_id;
    if (groups.length === 1) return groups[0].id;
    return '';
  }, [selectedGroup, resolvedBrandId, resolvedAgencyId, brands, agencies, user?.group_id, groups]);

  const resolvedBrandName = useMemo(() => {
    if (!resolvedBrandId) return '';
    return brands.find((item) => item.id === resolvedBrandId)?.name || '';
  }, [resolvedBrandId, brands]);

  const loadPriceMatrix = useCallback(async () => {
    if (!resolvedGroupId || !resolvedBrandId || !resolvedBrandName) {
      setRows([]);
      setGlobalDealerSharePct(0);
      return;
    }

    setLoading(true);
    try {
      const bulletinParams = {
        group_id: resolvedGroupId,
        brand_id: resolvedBrandId,
        latest_per_model: true,
        active_only: true,
        include_brand_defaults: true,
      };
      if (resolvedAgencyId) bulletinParams.agency_id = resolvedAgencyId;

      const [catalogRes, bulletinsRes] = await Promise.all([
        vehicleCatalogApi.getModels(resolvedBrandName, { allYears: true }),
        priceBulletinsApi.getAll(bulletinParams),
      ]);

      const catalogModels = (Array.isArray(catalogRes?.data?.items) ? catalogRes.data.items : [])
        .map((item) => ({
          model: String(item?.name || '').trim(),
          min_msrp: Number(item?.min_msrp || 0),
        }))
        .filter((item) => item.model)
        .sort((a, b) => a.model.localeCompare(b.model, 'es-MX'));

      const versionFetchFailures = [];
      const versionsByModel = await Promise.all(
        catalogModels.map(async (catalogModel) => {
          try {
            const versionsRes = await vehicleCatalogApi.getVersions(
              resolvedBrandName,
              catalogModel.model,
              { allYears: true },
            );
            const versions = Array.isArray(versionsRes?.data?.items) ? versionsRes.data.items : [];
            return {
              model: catalogModel.model,
              min_msrp: catalogModel.min_msrp,
              versions,
              fetch_failed: false,
            };
          } catch (error) {
            versionFetchFailures.push({
              model: catalogModel.model,
              status: Number(error?.response?.status || 0),
            });
            return {
              model: catalogModel.model,
              min_msrp: catalogModel.min_msrp,
              versions: [],
              fetch_failed: true,
            };
          }
        }),
      );

      const catalogRows = versionsByModel.flatMap((entry) => {
        if (entry.fetch_failed || !entry.versions.length) return [];
        const modelMinMsrp = Number(entry.min_msrp || 0);
        return entry.versions
          .map((versionItem) => ({
            model: entry.model,
            version: normalizeText(versionItem?.name || ''),
            min_msrp: Number(versionItem?.msrp || modelMinMsrp || 0),
          }))
          .filter((row) => row.version);
      });

      const bulletins = Array.isArray(bulletinsRes?.data) ? bulletinsRes.data : [];
      const pickPreferredBulletin = (existing, candidate) => {
        if (!existing) return candidate;
        if (candidate.__agency_specific && !existing.__agency_specific) return candidate;
        if (!candidate.__agency_specific && existing.__agency_specific) return existing;
        const candidateEffectiveFrom = String(candidate?.effective_from || '');
        const existingEffectiveFrom = String(existing?.effective_from || '');
        if (candidateEffectiveFrom > existingEffectiveFrom) return candidate;
        const candidateUpdatedAt = new Date(candidate?.updated_at || candidate?.created_at || 0).getTime();
        const existingUpdatedAt = new Date(existing?.updated_at || existing?.created_at || 0).getTime();
        if (candidateUpdatedAt > existingUpdatedAt) return candidate;
        return existing;
      };

      const bulletinByModelVersion = new Map();
      const bulletinByModel = new Map();
      bulletins.forEach((bulletin) => {
        const modelName = normalizeText(bulletin?.model || '');
        if (!modelName) return;
        const versionName = normalizeText(bulletin?.version || '');
        const isAgencySpecific = Boolean(resolvedAgencyId) && String(bulletin?.agency_id || '') === resolvedAgencyId;
        const enriched = { ...bulletin, __agency_specific: isAgencySpecific };
        const versionKey = makeBulletinKey(modelName, versionName);
        const currentVersionDoc = bulletinByModelVersion.get(versionKey);
        bulletinByModelVersion.set(versionKey, pickPreferredBulletin(currentVersionDoc, enriched));
        if (!versionName) {
          const modelKey = modelName.toLowerCase();
          const currentModelDoc = bulletinByModel.get(modelKey);
          bulletinByModel.set(modelKey, pickPreferredBulletin(currentModelDoc, enriched));
        }
      });

      const mergedRows = catalogRows
        .map((catalogRow) => {
          const exactKey = makeBulletinKey(catalogRow.model, catalogRow.version);
          const modelKey = catalogRow.model.toLowerCase();
          const bulletin = bulletinByModelVersion.get(exactKey) || bulletinByModel.get(modelKey);
          const catalogPrice = toNonNegativeNumber(catalogRow.min_msrp);
          const bulletinPrice = toNonNegativeNumber(bulletin?.msrp ?? catalogPrice);
          const storedBrandIncentive = toNonNegativeNumber(bulletin?.brand_bonus_amount || 0);
          const storedDealerIncentive = toNonNegativeNumber(bulletin?.dealer_bonus_amount || 0);
          const storedDealerPctRaw = bulletin?.dealer_share_percentage;
          const totalFromPrice = Math.max(catalogPrice - bulletinPrice, 0);
          const totalFromStored = storedBrandIncentive + storedDealerIncentive;
          const effectiveTotalIncentive = totalFromPrice > 0 ? totalFromPrice : totalFromStored;
          const effectivePrice = catalogPrice > 0 ? Math.max(catalogPrice - effectiveTotalIncentive, 0) : bulletinPrice;
          const inferredDealerPct = totalFromStored > 0
            ? clampPercentage((storedDealerIncentive / totalFromStored) * 100)
            : 0;
          const dealerSharePct = Number.isFinite(Number(storedDealerPctRaw))
            ? clampPercentage(storedDealerPctRaw)
            : inferredDealerPct;
          const computed = computeFromPrice(catalogPrice, effectivePrice, dealerSharePct);

          return {
            row_key: exactKey,
            model: catalogRow.model,
            version: catalogRow.version,
            min_msrp: catalogPrice,
            msrp: computed.msrp,
            brand_bonus_amount: computed.brand_bonus_amount,
            dealer_bonus_amount: computed.dealer_bonus_amount,
            dealer_share_percentage: computed.dealer_share_percentage,
          };
        })
        .sort((a, b) => {
          const modelSort = a.model.localeCompare(b.model, 'es-MX');
          if (modelSort !== 0) return modelSort;
          return a.version.localeCompare(b.version, 'es-MX');
        });

      if (versionFetchFailures.length > 0) {
        const sample = versionFetchFailures.slice(0, 5).map((item) => item.model).join(', ');
        toast.error(
          `No se cargaron versiones para ${versionFetchFailures.length} modelo(s): ${sample}. ` +
          'Reinicia el backend para aplicar la última versión de /catalog/versions.',
        );
      }

      const firstBulletin = bulletins[0];
      setMeta({
        bulletin_name: String(firstBulletin?.bulletin_name || `Boletín ${resolvedBrandName} ${todayDateString()}`),
        effective_from: String(firstBulletin?.effective_from || todayDateString()),
        effective_to: String(firstBulletin?.effective_to || ''),
        notes: String(firstBulletin?.notes || ''),
      });
      setRows(mergedRows);
      setGlobalDealerSharePct(clampPercentage(mergedRows[0]?.dealer_share_percentage ?? 0));
    } catch (error) {
      setRows([]);
      setGlobalDealerSharePct(0);
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo cargar el boletín de precios');
    } finally {
      setLoading(false);
    }
  }, [resolvedGroupId, resolvedBrandId, resolvedBrandName, resolvedAgencyId]);

  useEffect(() => {
    loadPriceMatrix();
  }, [loadPriceMatrix]);

  const handleMetaChange = (field, value) => {
    setMeta((prev) => ({ ...prev, [field]: value }));
  };

  const handleRowChange = (index, field, value) => {
    setRows((prev) => {
      const next = [...prev];
      const current = { ...(next[index] || {}) };
      const catalogPrice = toNonNegativeNumber(current.min_msrp || 0);
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) return prev;

      if (field === 'msrp') {
        const computed = computeFromPrice(catalogPrice, parsed, globalDealerSharePct);
        current.msrp = computed.msrp;
        current.brand_bonus_amount = computed.brand_bonus_amount;
        current.dealer_bonus_amount = computed.dealer_bonus_amount;
        current.dealer_share_percentage = computed.dealer_share_percentage;
      } else if (field === 'brand_bonus_amount') {
        const computed = computeFromBrandIncentive(catalogPrice, parsed, globalDealerSharePct);
        current.msrp = computed.msrp;
        current.brand_bonus_amount = computed.brand_bonus_amount;
        current.dealer_bonus_amount = computed.dealer_bonus_amount;
        current.dealer_share_percentage = computed.dealer_share_percentage;
      } else {
        current[field] = toNonNegativeNumber(parsed);
      }
      next[index] = current;
      return next;
    });
  };

  const applyModelIncentiveFromRow = (index) => {
    const modelName = rows[index]?.model || 'modelo';
    setRows((prev) => {
      const source = prev[index];
      if (!source) return prev;
      const targetModel = normalizeText(source.model);
      const desiredBrandIncentive = toNonNegativeNumber(source.brand_bonus_amount || 0);
      const updated = prev.map((row) => {
        if (normalizeText(row.model) !== targetModel) return row;
        const computed = computeFromBrandIncentive(row.min_msrp, desiredBrandIncentive, globalDealerSharePct);
        return {
          ...row,
          msrp: computed.msrp,
          brand_bonus_amount: computed.brand_bonus_amount,
          dealer_bonus_amount: computed.dealer_bonus_amount,
          dealer_share_percentage: computed.dealer_share_percentage,
        };
      });
      return updated;
    });
    toast.success(`Incentivo aplicado a todas las versiones de ${modelName}`);
  };

  const applyGlobalDealerShare = () => {
    const pct = clampPercentage(globalDealerSharePct);
    setGlobalDealerSharePct(pct);
    setRows((prev) => prev.map((row) => {
      const computed = computeFromPrice(row.min_msrp, row.msrp, pct);
      return {
        ...row,
        msrp: computed.msrp,
        brand_bonus_amount: computed.brand_bonus_amount,
        dealer_bonus_amount: computed.dealer_bonus_amount,
        dealer_share_percentage: computed.dealer_share_percentage,
      };
    }));
    toast.success(`Aporte dealer ${pct}% aplicado a todos los vehículos`);
  };

  const handleSave = async () => {
    if (!canEdit) {
      toast.error('Tu rol no puede editar boletines de precios');
      return;
    }
    if (!resolvedGroupId || !resolvedBrandId) {
      toast.error('Selecciona grupo y marca para guardar');
      return;
    }
    if (!rows.length) {
      toast.error('No hay modelos para guardar');
      return;
    }

    try {
      setSaving(true);
      const payload = {
        group_id: resolvedGroupId,
        brand_id: resolvedBrandId,
        agency_id: resolvedAgencyId || null,
        bulletin_name: String(meta.bulletin_name || '').trim() || null,
        effective_from: String(meta.effective_from || '').trim() || todayDateString(),
        effective_to: String(meta.effective_to || '').trim() || null,
        notes: String(meta.notes || '').trim() || null,
        items: rows.map((row) => ({
          model: row.model,
          version: row.version || null,
          msrp: Number(row.msrp || 0),
          transaction_price: null,
          brand_bonus_amount: Number(row.brand_bonus_amount || 0),
          brand_bonus_percentage: 0,
          dealer_bonus_amount: Number(row.dealer_bonus_amount || 0),
          dealer_share_percentage: clampPercentage(globalDealerSharePct),
        })),
      };

      const saveRes = await priceBulletinsApi.bulkUpsert(payload);
      const repricing = saveRes?.data?.repricing;
      if (repricing && Number(repricing.checked || 0) > 0) {
        toast.success(
          `Boletín guardado. Ventas revisadas: ${repricing.checked}. Recalculadas: ${repricing.repriced}.`,
        );
      } else {
        toast.success('Boletín de precios guardado');
      }
      await loadPriceMatrix();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo guardar el boletín');
    } finally {
      setSaving(false);
    }
  };

  const formatCurrency = (value) =>
    new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0,
    }).format(Number(value || 0));

  return (
    <div className="space-y-6" data-testid="prices-page">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Precios y Boletines
          </h1>
          <p className="text-muted-foreground">
            MSRP de catálogo, precio boletín e incentivos por modelo enviados por marca/armadora.
          </p>
        </div>
        <Button
          onClick={handleSave}
          disabled={!canEdit || saving || loading || !rows.length}
          className="bg-[#002FA7] hover:bg-[#002FA7]/90"
          data-testid="save-price-bulletin-btn"
        >
          {!canEdit ? 'Sin permiso de edición' : (saving ? 'Guardando...' : 'Guardar Boletín')}
        </Button>
      </div>

      <HierarchicalFilters filters={filters} includeSellers={false} />

      <Card className="border-border/40">
        <CardHeader>
          <CardTitle>Configuración de Boletín</CardTitle>
          <CardDescription>
            Define vigencia y valores por modelo para la marca seleccionada.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {!resolvedBrandName ? (
            <div className="text-sm text-muted-foreground">
              Selecciona una marca para gestionar precios.
            </div>
          ) : (
            <>
              <div className="text-sm text-muted-foreground">
                Marca: <span className="font-medium text-foreground">{resolvedBrandName}</span>
                {resolvedAgencyId ? ' · Alcance: Agencia seleccionada' : ' · Alcance: Marca (todas las agencias)'}
              </div>
              <div className="grid gap-3 md:grid-cols-4">
                <div className="space-y-1 md:col-span-2">
                  <Label>Nombre del boletín</Label>
                  <Input
                    value={meta.bulletin_name}
                    onChange={(e) => handleMetaChange('bulletin_name', e.target.value)}
                    disabled={!canEdit}
                    data-testid="price-bulletin-name-input"
                  />
                </div>
                <div className="space-y-1">
                  <Label>Vigencia desde</Label>
                  <Input
                    type="date"
                    value={meta.effective_from}
                    onChange={(e) => handleMetaChange('effective_from', e.target.value)}
                    disabled={!canEdit}
                    data-testid="price-bulletin-from-input"
                  />
                </div>
                <div className="space-y-1">
                  <Label>Vigencia hasta (opcional)</Label>
                  <Input
                    type="date"
                    value={meta.effective_to}
                    onChange={(e) => handleMetaChange('effective_to', e.target.value)}
                    disabled={!canEdit}
                    data-testid="price-bulletin-to-input"
                  />
                </div>
              </div>
              <div className="space-y-1">
                <Label>Notas</Label>
                <Input
                  value={meta.notes}
                  onChange={(e) => handleMetaChange('notes', e.target.value)}
                  disabled={!canEdit}
                  placeholder="Ej: Boletín oficial de armadora abril 2026"
                  data-testid="price-bulletin-notes-input"
                />
              </div>
            </>
          )}
        </CardContent>
      </Card>

      <Card className="border-border/40">
        <CardHeader>
          <CardTitle>Modelos y Precios</CardTitle>
          <CardDescription>
            Si cambias el precio boletín se recalcula incentivo marca. Si cambias incentivo marca se recalcula precio boletín.
            Usa "Aplicar al modelo" para copiar ese incentivo al resto de versiones del mismo modelo.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {loading ? (
            <Skeleton className="h-64 w-full" />
          ) : !resolvedBrandName ? (
            <div className="text-sm text-muted-foreground">
              Selecciona una marca para ver modelos.
            </div>
          ) : rows.length === 0 ? (
            <div className="text-sm text-muted-foreground">
              No hay modelos disponibles para esta marca.
            </div>
          ) : (
            <>
              <div className="mb-4 flex flex-col sm:flex-row sm:items-end gap-3">
                <div className="space-y-1">
                  <Label>Aporte dealer global %</Label>
                  <Input
                    type="number"
                    min="0"
                    max="100"
                    step="1"
                    value={globalDealerSharePct}
                    onChange={(e) => setGlobalDealerSharePct(clampPercentage(e.target.value))}
                    disabled={!canEdit}
                    className="w-40 text-right"
                    data-testid="price-global-dealer-share-input"
                  />
                </div>
                <Button
                  onClick={applyGlobalDealerShare}
                  disabled={!canEdit || loading || !rows.length}
                  variant="outline"
                  data-testid="price-apply-global-dealer-share-btn"
                >
                  Aplicar a todos
                </Button>
              </div>
              <div className="rounded-md border border-border/40 overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Modelo</TableHead>
                      <TableHead>Versión</TableHead>
                      <TableHead className="text-right">MSRP Catálogo</TableHead>
                      <TableHead className="text-right">Precio Boletín</TableHead>
                      <TableHead className="text-right">Incentivo Marca $</TableHead>
                      <TableHead className="text-right">Incentivo dealer $</TableHead>
                      <TableHead className="text-right">Acción Modelo</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {rows.map((row, index) => (
                      <TableRow key={row.row_key}>
                        <TableCell className="font-medium">{row.model}</TableCell>
                        <TableCell>{row.version || 'Sin versión'}</TableCell>
                        <TableCell className="text-right">{formatCurrency(row.min_msrp || 0)}</TableCell>
                        <TableCell className="text-right">
                          {canEdit ? (
                            <Input
                              type="number"
                              min="0"
                              step="100"
                              value={row.msrp}
                              onChange={(e) => handleRowChange(index, 'msrp', e.target.value)}
                              className="w-36 ml-auto text-right"
                              data-testid={`price-msrp-${makeTestIdSuffix(row.model, row.version)}`}
                            />
                          ) : (
                            formatCurrency(row.msrp || 0)
                          )}
                        </TableCell>
                        <TableCell className="text-right">
                          {canEdit ? (
                            <Input
                              type="number"
                              min="0"
                              step="100"
                              value={row.brand_bonus_amount}
                              onChange={(e) => handleRowChange(index, 'brand_bonus_amount', e.target.value)}
                              className="w-32 ml-auto text-right"
                              data-testid={`price-bonus-amount-${makeTestIdSuffix(row.model, row.version)}`}
                            />
                          ) : (
                            formatCurrency(row.brand_bonus_amount || 0)
                          )}
                        </TableCell>
                        <TableCell className="text-right">
                          {formatCurrency(row.dealer_bonus_amount || 0)}
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            onClick={() => applyModelIncentiveFromRow(index)}
                            disabled={!canEdit}
                            data-testid={`price-apply-model-${makeTestIdSuffix(row.model)}`}
                          >
                            Aplicar al modelo
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
              <div className="pt-4 flex justify-end">
                <Button
                  onClick={handleSave}
                  disabled={!canEdit || saving || loading || !rows.length}
                  className="bg-[#002FA7] hover:bg-[#002FA7]/90"
                  data-testid="save-price-bulletin-bottom-btn"
                >
                  {!canEdit ? 'Sin permiso de edición' : (saving ? 'Guardando...' : 'Guardar Boletín')}
                </Button>
              </div>
              <p className="pt-2 text-xs text-muted-foreground">
                Por default el aporte dealer global es 0%, así el 100% del incentivo lo absorbe la marca.
              </p>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
