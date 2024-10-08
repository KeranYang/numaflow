import { useEffect, useState, useCallback, useContext } from "react";
import { useFetch, Options } from "./fetch";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import {
  NamespacePipelineSummary,
  NamespaceSummaryData,
  NamespaceSummaryFetchProps,
  NamespaceSummaryFetchResult,
} from "../../types/declarations/namespace";

const rawDataToNamespaceSummary = (
  rawPipelineData: any[],
  rawIsbData: any[],
  rawMonoVertexData: any[]
): NamespaceSummaryData | undefined => {
  const pipelinesCount =
    (Array.isArray(rawPipelineData) ? rawPipelineData.length : 0) +
    (Array.isArray(rawMonoVertexData) ? rawMonoVertexData.length : 0);
  let pipelinesActiveCount = 0;
  let pipelinesInactiveCount = 0;
  let pipelinesHealthyCount = 0;
  let pipelinesWarningCount = 0;
  let pipelinesCriticalCount = 0;
  const isbsCount = Array.isArray(rawIsbData) ? rawIsbData.length : 0;
  let isbsActiveCount = 0;
  let isbsInactiveCount = 0;
  let isbsHealthyCount = 0;
  let isbsWarningCount = 0;
  let isbsCriticalCount = 0;
  const pipelineSummaries: NamespacePipelineSummary[] = [];
  Array.isArray(rawPipelineData) &&
    rawPipelineData?.forEach((pipeline: any) => {
      switch (pipeline.status) {
        case "healthy":
          pipelinesActiveCount++;
          pipelinesHealthyCount++;
          break;
        case "warning":
          pipelinesActiveCount++;
          pipelinesWarningCount++;
          break;
        case "critical":
          pipelinesActiveCount++;
          pipelinesCriticalCount++;
          break;
        case "inactive":
          pipelinesInactiveCount++;
          break;
        default:
          break;
      }
      // Add pipeline summary to array
      pipelineSummaries.push({
        name: pipeline.name,
        status: pipeline.status,
      });
    });
  // adding MonoVertex count to pipeline count
  Array.isArray(rawMonoVertexData) &&
    rawMonoVertexData?.forEach((monoVertex: any) => {
      switch (monoVertex.status) {
        case "healthy":
          pipelinesActiveCount++;
          pipelinesHealthyCount++;
          break;
        case "warning":
          pipelinesActiveCount++;
          pipelinesWarningCount++;
          break;
        case "critical":
          pipelinesActiveCount++;
          pipelinesCriticalCount++;
          break;
        case "inactive":
          pipelinesInactiveCount++;
          break;
        default:
          break;
      }
      // Add pipeline summary to array
      pipelineSummaries.push({
        name: monoVertex.name,
        status: monoVertex.status,
      });
    });
  Array.isArray(rawIsbData) &&
    rawIsbData?.forEach((isb: any) => {
      switch (isb.status) {
        case "healthy":
          isbsActiveCount++;
          isbsHealthyCount++;
          break;
        case "warning":
          isbsActiveCount++;
          isbsWarningCount++;
          break;
        case "critical":
          isbsActiveCount++;
          isbsCriticalCount++;
          break;
        case "inactive":
          isbsInactiveCount++;
          break;
        default:
          break;
      }
    });
  // TODO how to map ISB to pipeline?
  return {
    pipelinesCount,
    pipelinesActiveCount,
    pipelinesInactiveCount,
    pipelinesHealthyCount,
    pipelinesWarningCount,
    pipelinesCriticalCount,
    isbsCount,
    isbsActiveCount,
    isbsInactiveCount,
    isbsHealthyCount,
    isbsWarningCount,
    isbsCriticalCount,
    pipelineSummaries,
  };
};

const DATA_REFRESH_INTERVAL = 15000; // ms

export const useNamespaceSummaryFetch = ({
  namespace,
  loadOnRefresh = false,
  addError,
}: NamespaceSummaryFetchProps) => {
  const [options, setOptions] = useState<Options>({
    skip: false,
    requestKey: "",
  });

  const refresh = useCallback(() => {
    setOptions({
      skip: false,
      requestKey: "id" + Math.random().toString(16).slice(2),
    });
  }, []);

  const [results, setResults] = useState<NamespaceSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
    refresh,
  });

  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data: pipelineData,
    loading: pipelineLoading,
    error: pipelineError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespace}/pipelines`,
    undefined,
    options
  );
  const {
    data: isbData,
    loading: isbLoading,
    error: isbError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespace}/isb-services`,
    undefined,
    options
  );
  const {
    data: monoVertexData,
    loading: monoVertexLoading,
    error: monoVertexError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespace}/mono-vertices`,
    undefined,
    options
  );

  useEffect(() => {
    setInterval(() => {
      setOptions({
        skip: false,
        requestKey: "id" + Math.random().toString(16).slice(2),
      });
    }, DATA_REFRESH_INTERVAL);
  }, []);

  useEffect(() => {
    if (pipelineLoading || isbLoading || monoVertexLoading) {
      if (options?.requestKey === "" || loadOnRefresh) {
        // Only set loading true when first load or when loadOnRefresh is true
        setResults({
          data: undefined,
          loading: true,
          error: undefined,
          refresh,
        });
      }
      return;
    }
    if (pipelineError || isbError || monoVertexError) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: pipelineError || isbError || monoVertexError,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(pipelineError || isbError || monoVertexError);
      }
      return;
    }
    if (pipelineData?.errMsg || isbData?.errMsg || monoVertexData?.errMsg) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error:
            pipelineData?.errMsg || isbData?.errMsg || monoVertexData?.errMsg,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(
          pipelineData?.errMsg || isbData?.errMsg || monoVertexData?.errMsg
        );
      }
      return;
    }
    if (pipelineData && isbData && monoVertexData) {
      const pipelineMap = pipelineData?.data?.reduce((map: any, obj: any) => {
        map[obj.name] = obj;
        return map;
      }, {});
      const isbMap = isbData?.data?.reduce((map: any, obj: any) => {
        map[obj.name] = obj;
        return map;
      }, {});
      const monoVertexMap = monoVertexData?.data?.reduce(
        (map: any, obj: any) => {
          map[obj.name] = obj;
          return map;
        },
        {}
      );
      const nsSummary = rawDataToNamespaceSummary(
        pipelineData?.data,
        isbData?.data,
        monoVertexData?.data
      );
      setResults({
        data: nsSummary,
        pipelineRawData: pipelineMap,
        isbRawData: isbMap,
        monoVertexRawData: monoVertexMap,
        loading: false,
        error: undefined,
        refresh,
      });
      return;
    }
  }, [
    pipelineData,
    isbData,
    monoVertexData,
    pipelineLoading,
    isbLoading,
    monoVertexLoading,
    pipelineError,
    isbError,
    monoVertexError,
    loadOnRefresh,
    options,
    refresh,
    addError,
  ]);

  return results;
};
