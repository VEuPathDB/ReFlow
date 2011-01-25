package ReFlow::Controller::ResolveTemplate;

use strict;
use Data::Dumper;

my $MACROSYM = '**';

# scan template file
# find all template macros and substitute in value
# also grab all step and subgraph names
sub resolve {
  my ($xmlFile, $rtcFile, $name) = @_;

  my $templateConfig = parseRtcFile($name, $rtcFile);

  open(XML, $xmlFile) || die "Can't open xml_file '$xmlFile'\n";
  my @newXml;
  my @stepNames;
  while (<XML>) {
    next if /\<workflowGraph/;
    next if /\<\/workflowGraph/;
    die "Error: template XML file '$xmlFile' not allowed to contain a <templateInstance>\n"
      if /\<templateInstance/;
    die "Error: template XML file  '$xmlFile' not allowed to contain a <templateDepends>\n"
      if /\<templateDepends/;
    if (/\<step|\<subgraph/) {
      if (/name\s*\=\s*\"(.*?)\"/) {
	push(@stepNames, $1);
      } else {
	die "Error: line $. of template XML file '$xmlFile' has a step or subgraph call without a name= attribute.  These must be on the same line in a template file\n";
      }
    }
    my $fixedLine =
      substituteTemplateMacros($_, $templateConfig, $name, $rtcFile, $xmlFile);
    push(@newXml, $fixedLine);
  }
  close(XML);
  return (join("", @newXml), \@stepNames);
}

sub parseRtcFile {
  my ($name, $rtcFile) = @_;

  my $config;
  my $found;
  my $done;
  open(RTC, $rtcFile) || die "Can't open rtc_file '$rtcFile'\n";
  $config->{NAME} = $name;
  while(<RTC>) {
    chomp;
    next if /^\s*$/;
    if (/^\>$name/) {
      die "Error: duplicate stanza for '$name' found in rtc file '$rtcFile'\n" if $done;
      $found = 1;
    } elsif ($found && /\/\//) {
      $done = 1;
    } elsif ($found && !$done && /^\>/) {
      die "Error: stanza for '$name' must end in a line with '//' in rtc file '$rtcFile'\n";
    } elsif ($found && !$done) {
      /(\S+?)\s*\=\s*(.*)/ || die "Error: invalid format on line $. of rtc_file '$rtcFile'\n";
      my $key = $1;
      die "Error: duplicate key '$key' found in stanza for '$name' in rtc file '$rtcFile'\n"
	if $config->{$key};
      $config->{$key} = $2;
    }
  }
  close(RTC);
  print Dumper $config;
  return $config;
}

sub substituteTemplateMacros {
  my ($line, $config, $name, $rtcFile, $xmlFile) = @_;

  if ($line =~ /\<subgraph/ || $line =~ /<step/) {
    $line =~ s/name\s*=\s*\"(.+?)\"/name=\"$name$1\"/;
  }

  return $line unless $line =~ m/\Q$MACROSYM\E/;

  foreach my $key (keys(%$config)) {
    $line =~ s/\Q$MACROSYM$key$MACROSYM\E/$config->{$key}/g;
  }
  die 
"Error trying to resolve <templateInstance> with name '$name'.
    template file: $xmlFile
    rtc file:      $rtcFile
Can't find a config property for a template macro on line $. of template file:\n $line\n" if $line =~ /\Q$MACROSYM\E/;
  return $line;
}

1;
