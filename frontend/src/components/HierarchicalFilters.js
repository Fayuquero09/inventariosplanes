import { useState, useEffect } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { groupsApi, brandsApi, agenciesApi, sellersApi } from '../lib/api';
import { Card, CardContent } from '../components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../components/ui/select';
import { Buildings, Factory, Storefront, User, CaretRight } from '@phosphor-icons/react';

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

  const isSuperUser = user?.role === 'app_admin' || user?.role === 'app_user';
  const canSelectGroup = isSuperUser && groups.length > 1;

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
    setSelectedBrand('all');
    setSelectedAgency('all');
    setSelectedSeller('all');
  }, [selectedGroup]);

  useEffect(() => {
    setSelectedAgency('all');
    setSelectedSeller('all');
  }, [selectedBrand]);

  useEffect(() => {
    setSelectedSeller('all');
  }, [selectedAgency]);

  // Notify parent of filter changes
  useEffect(() => {
    if (onFilterChange) {
      onFilterChange(getFilterParams());
    }
  }, [selectedGroup, selectedBrand, selectedAgency, selectedSeller]);

  // Filter brands by selected group
  const filteredBrands = selectedGroup !== 'all'
    ? brands.filter(b => b.group_id === selectedGroup)
    : brands;

  // Filter agencies by selected brand (or group)
  const filteredAgencies = selectedBrand !== 'all'
    ? agencies.filter(a => a.brand_id === selectedBrand)
    : selectedGroup !== 'all'
      ? agencies.filter(a => a.group_id === selectedGroup)
      : agencies;

  // Get filter params for API calls
  const getFilterParams = () => {
    const params = {};
    if (selectedGroup !== 'all') params.group_id = selectedGroup;
    if (selectedBrand !== 'all') params.brand_id = selectedBrand;
    if (selectedAgency !== 'all') params.agency_id = selectedAgency;
    if (selectedSeller !== 'all') params.seller_id = selectedSeller;
    return params;
  };

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
    isSuperUser, canSelectGroup,
    filteredBrands, filteredAgencies,
    selectedGroupObj, selectedBrandObj, selectedAgencyObj, selectedSellerObj,
    // Helpers
    getFilterParams
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
    canSelectGroup,
    selectedGroupObj, selectedBrandObj, selectedAgencyObj, selectedSellerObj
  } = filters;

  return (
    <>
      <Card className="border-border/40">
        <CardContent className="p-4">
          <div className="flex flex-wrap gap-3">
            {/* Group Filter */}
            {canSelectGroup ? (
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
            ) : groups.length === 1 && (
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
                disabled={canSelectGroup && selectedGroup === 'all'}
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
