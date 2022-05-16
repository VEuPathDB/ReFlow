use strict;
use warnings;

use lib "$ENV{GUS_HOME}/lib/perl";

use ReFlow::StepClasses::RunAndMonitorNextflow;
use Test::More;


my $tail; 
is(ReFlow::StepClasses::RunAndMonitorNextflow::tailLooksOk($tail), undef, "Null case");

$tail = <<EOF;
Sep-22 20:14:20.132 [main] DEBUG nextflow.Session - Session await > all barriers passed
Sep-22 20:14:20.169 [main] DEBUG nextflow.trace.WorkflowStatsObserver - Workflow completed > WorkflowStats[succeededCount=540; failedCount=13; ignoredCount=0; cachedCount=69; pendingCount=0;
 submittedCount=0; runningCount=0; retriesCount=13; abortedCount=0; succeedDuration=1d 1h 14m 19s; failedDuration=1m 16s; cachedDuration=56m 43s;loadCpus=0; loadMemory=0; peakRunning=33; pea
kCpus=33; peakMemory=0; ]
Sep-22 20:14:20.169 [main] DEBUG nextflow.trace.TraceFileObserver - Flow completing -- flushing trace file
Sep-22 20:14:20.523 [main] DEBUG nextflow.CacheDB - Closing CacheDB done
Sep-22 20:14:20.558 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye
EOF

is(ReFlow::StepClasses::RunAndMonitorNextflow::tailLooksOk($tail), 1, "Retries prevail");

$tail = <<EOF;
Sep-22 17:26:17.630 [main] WARN  n.processor.TaskPollingMonitor - Killing pending tasks (25)
Sep-22 17:26:18.144 [main] DEBUG nextflow.trace.WorkflowStatsObserver - Workflow completed > WorkflowStats[succeededCount=69; failedCount=28; ignoredCount=0; cachedCount=0; pendingCount=49; 
submittedCount=2; runningCount=-2; retriesCount=27; abortedCount=25; succeedDuration=56m 43s; failedDuration=2m; cachedDuration=0ms;loadCpus=-2; loadMemory=0; peakRunning=25; peakCpus=25; pe
akMemory=0; ]
Sep-22 17:26:18.144 [main] DEBUG nextflow.trace.TraceFileObserver - Flow completing -- flushing trace file
Sep-22 17:26:18.301 [main] DEBUG nextflow.CacheDB - Closing CacheDB done
Sep-22 17:26:18.341 [main] DEBUG nextflow.script.ScriptRunner - > Execution complete -- Goodbye
EOF

is(ReFlow::StepClasses::RunAndMonitorNextflow::tailLooksOk($tail), '', "Retried but failed");


done_testing;
