import { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { groupsApi, brandsApi, agenciesApi, sellersApi } from '../lib/api';
import { Card, CardContent } from '../components/ui/card';
import { Button } from '../components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../components/ui/select';
import { Buildings, Factory, Storefront, User, CaretRight, ArrowsLeftRight } from '@phosphor-icons/react';

const FILTER_MODES = {
  GROUP_BRAND: 'group_brand',
  BRAND_AGENCY: 'brand_agency'
};

export function useHierarchicalFilters(options = {}) {
  const { user } = useAuth();
  const { includeSellers = false, onFilterChange } = options;

  const [groups, setGroups] = useState([]);
  const [brands, setBrands] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [sellers, setSellers] = useState([]);

  const [selectedGroup, setSelectedGroup] = useState('all');
  const [selectedBrand, setSelectedBrand] = useState('all');
  const [selectedAgency, setSelectedAgency] = useState('all');
  const [selectedSeller, setSelectedSeller] = useState('all');
  const [filterMode, setFilterMode] = useState(FILTER_MODES.GROUP_BRAND);

  const isSuperUser = user?.role === 'app_admin' || user?.role === 'app_user';
  const canSelectGroup = isSuperUser && groups.length > 1;
  const canSwitchHierarchy = isSuperUser;
  const isBrandAgencyMode = canSwitchHierarchy && filterMode === FILTER_MODES.BRAND_AGENCY;

  // Load filter options
  useEffect(() => {
    const loadFilterOptions = async () => {
      try {
        const [groupsRes, brandsRes, agenciesRes] = await Promise.all([
          groupsApi.getAll(),
          brandsApi.getAll(),
          agenciesApi.getAll()
        ]);
        setGroups(groupsRes.data);
        setBrands(brandsRes.data);
        setAgencies(agenciesRes.data);

        // Auto-select group for non-super users
        if (!isSuperUser && groupsRes.data.length === 1) {
          setSelectedGroup(groupsRes.data[0].id);
        }
      } catch (error) {
        console.error('Error loading filter options:', error);
      }
    };
    loadFilterOptions();
  }, [isSuperUser]);

  // Load sellers when agency changes
  useEffect(() => {
    if (!includeSellers) return;
    
    const loadSellers = async () => {
      if (selectedAgency !== 'all') {
        try {
          const res = await sellersApi.getAll({ agency_id: selectedAgency });
          setSellers(res.data);
        } catch (error) {
          setSellers([]);
        }
      } else {
        setSellers([]);
        setSelectedSeller('all');
      }
    };
    loadSellers();
  }, [selectedAgency, includeSellers]);

  // Reset cascading filters
  useEffect(() => {
    if (isBrandAgencyMode) return;
    setSelectedBrand('all');
    setSelectedAgency('all');
    setSelectedSeller('all');
  }, [selectedGroup, isBrandAgencyMode]);

  useEffect(() => {
    setSelectedAgency('all');
    setSelectedSeller('all');
  }, [selectedBrand]);

  useEffect(() => {
    setSelectedSeller('all');
  }, [selectedAgency]);

  // Keep hierarchy mode scoped to super users and reset dependent filters when mode changes
  useEffect(() => {
    if (!canSwitchHierarchy && filterMode !== FILTER_MODES.GROUP_BRAND) {
      setFilterMode(FILTER_MODES.GROUP_BRAND);
      return;
    }
    if (!canSwitchHierarchy) return;

    setSelectedAgency('all');
    setSelectedSeller('all');

    if (filterMode === FILTER_MODES.BRAND_AGENCY) {
      setSelectedGroup('all');
    } else {
      setSelectedBrand('all');
    }
  }, [canSwitchHierarchy, filterMode]);

  // Get filter params for API calls
  const getFilterParams = useCallback(() => {
    const params = {};
    if (!isBrandAgencyMode && selectedGroup !== 'all') params.group_id = selectedGroup;
    if (selectedBrand !== 'all') params.brand_id = selectedBrand;
    if (selectedAgency !== 'all') params.agency_id = selectedAgency;
    if (selectedSeller !== 'all') params.seller_id = selectedSeller;
    return params;
  }, [selectedGroup, selectedBrand, selectedAgency, selectedSeller, isBrandAgencyMode]);

  // Notify parent of filter changes
  useEffect(() => {
    if (onFilterChange) {
      onFilterChange(getFilterParams());
    }
  }, [onFilterChange, getFilterParams]);

  // Filter brands by selected group
  const filteredBrands = !isBrandAgencyMode && selectedGroup !== 'all'
    ? brands.filter((b) => b.group_id === selectedGroup)
    : brands;

  // Filter agencies by selected brand (or group)
  const filteredAgencies = selectedBrand !== 'all'
    ? agencies.filter((a) => a.brand_id === selectedBrand)
    : !isBrandAgencyMode && selectedGroup !== 'all'
      ? agencies.filter((a) => a.group_id === selectedGroup)
      : agencies;

  // Get selected entities for breadcrumb
  const selectedGroupObj = groups.find(g => g.id === selectedGroup);
  const selectedBrandObj = brands.find(b => b.id === selectedBrand);
  const selectedAgencyObj = agencies.find(a => a.id === selectedAgency);
  const selectedSellerObj = sellers.find(s => s.id === selectedSeller);

  return {
    // States
    groups, brands, agencies, sellers,
    selectedGroup, selectedBrand, selectedAgency, selectedSeller,
    setSelectedGroup, setSelectedBrand, setSelectedAgency, setSelectedSeller,
    // Computed
    isSuperUser, canSelectGroup, canSwitchHierarchy, filterMode, isBrandAgencyMode,
    filteredBrands, filteredAgencies,
    selectedGroupObj, selectedBrandObj, selectedAgencyObj, selectedSellerObj,
    // Helpers
    setFilterMode, getFilterParams
  };
}

export function HierarchicalFilters({
  filters,
  includeSellers = false,
  showBreadcrumb = true
}) {
  const {
    groups, filteredBrands, filteredAgencies, sellers,
    selectedGroup, selectedBrand, selectedAgency, selectedSeller,
    setSelectedGroup, setSelectedBrand, setSelectedAgency, setSelectedSeller,
    canSelectGroup, canSwitchHierarchy, filterMode, setFilterMode, isBrandAgencyMode,
    selectedGroupObj, selectedBrandObj, selectedAgencyObj, selectedSellerObj
  } = filters;

  return (
    <>
      <Card className="border-border/40">
        <CardContent className="p-4">
          <div className="flex flex-wrap gap-3">
            {canSwitchHierarchy && (
              <div className="flex flex-col gap-1">
                <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                  <ArrowsLeftRight size={12} /> Vista
                </label>
                <div className="flex gap-2">
                  <Button
                    type="button"
                    size="sm"
                    variant={filterMode === FILTER_MODES.GROUP_BRAND ? 'default' : 'outline'}
                    className={filterMode === FILTER_MODES.GROUP_BRAND ? 'bg-[#002FA7] hover:bg-[#002FA7]/90' : ''}
                    onClick={() => setFilterMode(FILTER_MODES.GROUP_BRAND)}
                    data-testid="filter-mode-group-brand"
                  >
                    Grupo - Marca
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    variant={filterMode === FILTER_MODES.BRAND_AGENCY ? 'default' : 'outline'}
                    className={filterMode === FILTER_MODES.BRAND_AGENCY ? 'bg-[#2A9D8F] hover:bg-[#2A9D8F]/90' : ''}
                    onClick={() => setFilterMode(FILTER_MODES.BRAND_AGENCY)}
                    data-testid="filter-mode-brand-agency"
                  >
                    Marca - Agencia
                  </Button>
                </div>
              </div>
            )}

            {/* Group Filter */}
            {canSelectGroup && !isBrandAgencyMode ? (
              <div className="flex flex-col gap-1">
                <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                  <Buildings size={12} /> Grupo
                </label>
                <Select value={selectedGroup} onValueChange={setSelectedGroup}>
                  <SelectTrigger className="w-[180px]" data-testid="filter-group">
                    <SelectValue placeholder="Todos los grupos" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos los grupos</SelectItem>
                    {groups.map((group) => (
                      <SelectItem key={group.id} value={group.id}>
                        {group.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            ) : !isBrandAgencyMode && groups.length === 1 && (
              <div className="flex flex-col gap-1">
                <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                  <Buildings size={12} /> Grupo
                </label>
                <div className="h-10 px-3 py-2 rounded-md border border-border bg-muted/30 flex items-center text-sm font-medium">
                  {groups[0]?.name}
                </div>
              </div>
            )}

            {/* Brand Filter */}
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                <Factory size={12} /> Marca
              </label>
              <Select
                value={selectedBrand}
                onValueChange={setSelectedBrand}
                disabled={!isBrandAgencyMode && canSelectGroup && selectedGroup === 'all'}
              >
                <SelectTrigger className="w-[180px]" data-testid="filter-brand">
                  <SelectValue placeholder="Todas las marcas" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todas las marcas</SelectItem>
                  {filteredBrands.map((brand) => (
                    <SelectItem key={brand.id} value={brand.id}>
                      {brand.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Agency Filter */}
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                <Storefront size={12} /> Agencia
              </label>
              <Select
                value={selectedAgency}
                onValueChange={setSelectedAgency}
                disabled={selectedBrand === 'all'}
              >
                <SelectTrigger className="w-[180px]" data-testid="filter-agency">
                  <SelectValue placeholder="Todas las agencias" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todas las agencias</SelectItem>
                  {filteredAgencies.map((agency) => (
                    <SelectItem key={agency.id} value={agency.id}>
                      {agency.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Seller Filter */}
            {includeSellers && (
              <div className="flex flex-col gap-1">
                <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                  <User size={12} /> Vendedor
                </label>
                <Select
                  value={selectedSeller}
                  onValueChange={setSelectedSeller}
                  disabled={selectedAgency === 'all'}
                >
                  <SelectTrigger className="w-[180px]" data-testid="filter-seller">
                    <SelectValue placeholder="Todos los vendedores" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos los vendedores</SelectItem>
                    {sellers.map((seller) => (
                      <SelectItem key={seller.id} value={seller.id}>
                        {seller.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Breadcrumb */}
      {showBreadcrumb && (selectedGroupObj || selectedBrandObj || selectedAgencyObj || selectedSellerObj) && (
        <FilterBreadcrumb
          group={selectedGroupObj}
          brand={selectedBrandObj}
          agency={selectedAgencyObj}
          seller={selectedSellerObj}
        />
      )}
    </>
  );
}

export function FilterBreadcrumb({ group, brand, agency, seller }) {
  const parts = [];
  if (group) parts.push({ icon: Buildings, label: group.name });
  if (brand) parts.push({ icon: Factory, label: brand.name });
  if (agency) parts.push({ icon: Storefront, label: agency.name });
  if (seller) parts.push({ icon: User, label: seller.name });

  if (parts.length === 0) return null;

  return (
    <div className="flex items-center gap-2 text-sm text-muted-foreground">
      <span>Viendo:</span>
      {parts.map((part, index) => {
        const Icon = part.icon;
        return (
          <span key={index} className="flex items-center gap-1">
            {index > 0 && <CaretRight size={12} />}
            <Icon size={14} />
            <span className="font-medium text-foreground">{part.label}</span>
          </span>
        );
      })}
    </div>
  );
}
