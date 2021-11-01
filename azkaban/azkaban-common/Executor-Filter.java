



// ExecutorFilter的静态代码块中, 即包含价值相关配置



ExecutorFilter.static{
	
	filterRepository = new HashMap<>();
    filterRepository.put(STATICREMAININGFLOWSIZE_FILTER_NAME, getStaticRemainingFlowSizeFilter());
	
    filterRepository.put(MINIMUMFREEMEMORY_FILTER_NAME, getMinimumReservedMemoryFilter());{//ExecutorFilter.getMinimumReservedMemoryFilter()
		return FactorFilter
        .create(MINIMUMFREEMEMORY_FILTER_NAME, new FactorFilter.Filter<Executor, ExecutableFlow>() {
          private static final int MINIMUM_FREE_MEMORY = 6 * 1024;
          @Override
          public boolean filterTarget(final Executor filteringTarget,
              final ExecutableFlow referencingObject) {
            if (null == filteringTarget) {
              logger.debug(String.format("%s : filtering out the target as it is null.", MINIMUMFREEMEMORY_FILTER_NAME));
              return false;
            }

            final ExecutorInfo stats = filteringTarget.getExecutorInfo();
            if (null == stats) {
              logger.debug(String.format("%s : filtering out %s as it's stats is unavailable.", MINIMUMFREEMEMORY_FILTER_NAME, filteringTarget.toString()));
              return false;
            }
            return stats.getRemainingMemoryInMB() > MINIMUM_FREE_MEMORY;
          }
        });
	}
    filterRepository.put(CPUSTATUS_FILTER_NAME, getCpuStatusFilter());
}







