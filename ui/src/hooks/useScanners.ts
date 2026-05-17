import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, type ScannerInfo } from '../api';

const SCANNERS_KEY = ['scanners'] as const;

/**
 * Scanner enumeration is expensive (calls `scanimage -L` and probes eSCL
 * endpoints), so:
 *   - the server caches probe results for 5 minutes,
 *   - this hook serves the cached list immediately on mount,
 *   - a background refetch every 2 minutes keeps it eventually-fresh,
 *   - the returned `refresh` mutation forces a full re-probe (?refresh=true)
 *     and updates the query cache so all consumers see the new data.
 */
export function useScannersQuery() {
  const qc = useQueryClient();

  const query = useQuery({
    queryKey: SCANNERS_KEY,
    queryFn: () => api.listScanners(),
    staleTime: 5 * 60_000,
    refetchInterval: 2 * 60_000,
    refetchOnMount: false,
  });

  const refresh = useMutation({
    mutationFn: () => api.listScanners(true),
    onSuccess: (data: ScannerInfo[]) => qc.setQueryData(SCANNERS_KEY, data),
  });

  return { query, refresh };
}
