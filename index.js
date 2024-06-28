const fs = require('fs');
const prompts = require('prompts');

(async () => {
  const filePath = process.argv[2];
  const traces = loadTraces(filePath);
  const trace = traces.data[0];
  
  // Extract processes / service names
  const processesByServiceName = extractProcesses(trace);
  const spanCountPerProcess = countProcessesSpans(trace);
  
  // Let user select services to keep
  const servicesToKeepInTrace = await prompts({
    type: 'multiselect',
    name: 'value',
    message: 'Pick services to keep in trace',
    choices: Array.from(processesByServiceName.keys()).map(serviceName => {
      const spanCount = processesByServiceName.get(serviceName).map(process => spanCountPerProcess.get(process)).reduce((sum, a) => sum + a, 0);;
      return {
        title: `${serviceName} [${spanCount} spans]`,
        value: serviceName,
        selected: true };
    }),
    hint: '- Space to select. Return to submit'
  });

  // Transform into processes to keep
  const processesToKeepInTrace = servicesToKeepInTrace.value.flatMap(serviceName => processesByServiceName.get(serviceName));

  // Filter the trace to keep only relevant spans
  filterTraceWithServicesToKeep(trace, processesToKeepInTrace);

  // Output filtered traces
  fs.writeFileSync('filtered_trace.json', JSON.stringify(traces), 'utf8');
})();

function loadTraces(filePath) {
  var content = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(content);
}

function extractProcesses(trace) {
  const processesByServiceName = new Map();
  for (const process in trace.processes) {
    const serviceName = trace.processes[process].serviceName;
    if (processesByServiceName.get(serviceName) === undefined) {
      processesByServiceName.set(serviceName, []);
    }
    processesByServiceName.get(serviceName).push(process);
  }
  return processesByServiceName;
}

function countProcessesSpans(trace) {
  const spanCountPerProcess = new Map();
  trace.spans.forEach(span => {
    const process = span.processID;
    if (spanCountPerProcess.get(process) === undefined) {
      spanCountPerProcess.set(process, 0);
    }
    spanCountPerProcess.set(process, spanCountPerProcess.get(process) + 1);
  });
  return spanCountPerProcess;
}

function filterTraceWithServicesToKeep(trace, processesToKeep) {
  const processesSet = new Set(processesToKeep);

  // Remap parent spans to only target spans to keep
  const allSpans = new Map();
  trace.spans.forEach(span => {
    allSpans.set(span.spanID, span);
  });
  trace.spans.forEach(span => {
    if (span.references.length !== 1 || span.references[0].refType !== 'CHILD_OF') {
      return;
    }
    while (true) {
      const parentSpanId = span.references[0].spanID;
      const parentSpan = allSpans.get(parentSpanId);
      if (parentSpan === undefined) {
        return;
      }
      if (processesSet.has(parentSpan.processID)) {
        return;
      }
      if (parentSpan.references.length !== 1 || parentSpan.references[0].refType !== 'CHILD_OF') {
        return;
      }
      const newParentSpanId = parentSpan.references[0].spanId;
      span.references[0].spanID = newParentSpanId;
    }
  });

  // filter out unwanted spans
  const filteredSpans = trace.spans.filter(span => processesSet.has(span.processID));

  trace.spans = filteredSpans;
}
