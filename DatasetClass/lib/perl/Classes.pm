package ReFlow::DatasetClass::Classes;

use strict;

use XML::Simple;
use Data::Dumper;
use ReFlow::DatasetLoader::DatasetLoaders;

sub new {
  my ($class, $classesFile) = @_;

  my $self = {classesFile => $classesFile};

  bless($self,$class);

  $self->{xml} = new XML::Simple();

  $self->_parseXmlFile($classesFile);

  return $self;
}

sub validateClassName {
  my ($self, $className) = @_;

  return $self->{data}->{datasetClass}->{$className};
}

sub getClassesFile {
    my ($self) = @_;
    return $self->{classesFile};
}

# get list of templates used
sub getTemplate2Classes {
  my ($self, $classNamesUsed) = @_;
  my %template2classes;
  foreach my $className (@$classNamesUsed) {
    my $graphTemplateFiles = $self->{data}->{datasetClass}->{$className}->{graphTemplateFile};
#    print STDERR Dumper $graphTemplateFiles;
    foreach my $graphTemplateFile (@{$graphTemplateFiles}) {
      my $templateFileName = $graphTemplateFile->{name};
      push(@{$template2classes{$templateFileName}}, $className);
    }
  }
  return \%template2classes;
}

sub getClass {
    my ($self, $className) = @_;
    my $class = $self->{data}->{datasetClass}->{$className};
    die "Can't find class with name '$className' in classes file $self->{classesFile}\n" unless $class;
    return $class;
}

sub getClassGraphFileDir {
    my ($self, $className) = @_;
    my $class = $self->getClass($className);
    return $class->{graphFile}->{dir};
}

sub getDatasetLoaderText {
    my ($self, $dataset) = @_;
    my $class = $self->getClass($dataset->{class});
    my $datasetLoader;
    $datasetLoader->{datasetLoader} = $class->{datasetLoader}; # hack a little to outsmart XMLout
    return "" unless $datasetLoader->{datasetLoader};

    my $rawDatasetLoaderText = $self->{xml}->XMLout($datasetLoader);

    # jump through some hoops to lose weird lines out put by XMLout
    # probably a better way to do this
    # also, add CDATA to URL
#    <externalDbIdUrl>http://www.pberghei.eu/index.php?cat=geneid&q=EXTERNAL_ID_HERE</externalDbIdUrl>
    my @lines = split(/\n/,$rawDatasetLoaderText);
    my @lines2;
    my $cdatastart = '<![CDATA[';
    my $cdataend = ']]';
    foreach my $line (@lines) {
      next if $line =~ /^\<opt/;
      next if $line =~ /^\<\/opt/;
      next if $line =~ /^$/;
      if ($line =~ m|<externalDbIdUrl>(.*)</externalDbIdUrl>| && $line !~ /CDATA/) {
	$line = "     <externalDbIdUrl><![CDATA[$1]]></externalDbIdUrl>";
      }
      push(@lines2,$line);
    }
    my $rawDatasetLoaderText2 = join("\n", @lines2);

    my ($datasetLoaderText, $err) = substitutePropsIntoXmlText($rawDatasetLoaderText2, $dataset);
    if ($err) {
	die "Error: in classes file $self->{classesFile}, the <datasetLoader> element in class '$dataset->{class}' contains an invalid macro:\n$err\n";
    }
    return "$datasetLoaderText\n";
}


sub getDatasetPropertiesText {
    my ($self, $dataset, $datasetClassCategories) = @_;
    my $class = $self->getClass($dataset->{class});

    my $className = $dataset->{class};

    return "" unless $class->{datasetLoader};

    #print Dumper $class->{datasetLoader};

    my $rawName = $class->{datasetLoader}->[0]->{datasetName};

    my ($name, $err) = substitutePropsIntoXmlText($rawName, $dataset);
    if ($err) {
	die "Error: in classes file $self->{classesFile}, the <datasetLoader> element in class '$dataset->{class}' contains an invalid macro:\n$err\n";
    }

    my $project = $dataset->{prop}->{projectName}->{content};
    my $fullName = defined $project ? "$project:$name" : $name;

    my @props = ("datasetLoaderName=$fullName");

    my $datasetClassCategory = $datasetClassCategories->{$className}->{categoryDisplayName};
    push @props, "datasetClassCategory=$datasetClassCategory";

    my $datasetClassCategoryIri = $datasetClassCategories->{$className}->{categoryIri};
    push @props, "datasetClassCategoryIri=$datasetClassCategoryIri";

    foreach my $propKey (keys(%{$dataset->{prop}})) {
      my $propValue = $dataset->{prop}->{$propKey}->{content};
      push(@props, "$propKey=$propValue") unless($propKey eq 'projectName' && !$propValue);
    }
    return join("\n", @props);
}

# static method
sub substitutePropsIntoXmlText {
    my ($xmlText, $dataset) = @_;

    my $newXmlText = $xmlText;
    foreach my $propKey (keys(%{$dataset->{prop}})) {
#      print STDERR "prop: $propKey\n";
      my $propValue = $dataset->{prop}->{$propKey}->{content};
      $newXmlText =~ s/\$\{$propKey\}/$propValue/g;
    }

    my $err = 0;
    if ($newXmlText =~ /(\$\{.*?\})/) {
	$err = $1;
    }
    return ($newXmlText, $err);
}

sub _parseXmlFile {
  my ($self, $classesFile) = @_;

  # need to force all elements to be an array so that when we 
  # use XMLout to print xml text, they stay as elements
  my $fa = ReFlow::DatasetLoader::DatasetLoaders::getForceArray();
  my $forceArray = ['graphTemplateFile', 'prop','datasetClass', ,'manualGet', 'pluginArgs', 'externalDbIdUrl', @$fa];

  $self->{data} = eval{ $self->{xml}->XMLin($classesFile, SuppressEmpty => undef, KeyAttr=>'class', ForceArray=>$forceArray) };

#  print STDERR Dumper $self->{data};
  die "$@\nerror processing classes XML file $classesFile\n" if($@);
}

1;
