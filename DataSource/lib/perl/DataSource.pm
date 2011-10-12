package ReFlow::DataSource::DataSource;

use strict;
use Data::Dumper;

sub new {
  my ($class, $resourceName, $parsedXml, $dataSources) = @_;

  my $self = {parsedXml => $parsedXml,
              resourceName => $resourceName,
              dataSources => $dataSources,
              version => $parsedXml->{version},
              plugin =>  $parsedXml->{plugin},
              wgetArgs => $parsedXml->{wgetArgs}->{content},
              wgetUrl => $parsedXml->{wgetArgs}->{url},
              manualGet => $parsedXml->{manualGet},
              # NOTE: these 2 are initialized to empty arrayrefs if undef
              unpacks => $parsedXml->{unpack} || [],
              getAndUnpackOutput => $parsedXml->{getAndUnpackOutput} || [],
             };

  bless($self,$class);

  return $self;
}


sub getDataSources {
  my ($self) = @_;

  return $self->{dataSources};
}

sub getParsedXml {
  my ($self) = @_;

  return $self->{parsedXml};
}


sub getName {
    my ($self) = @_;

    return $self->{resourceName};
}

sub getVersion {
    my ($self) = @_;

    return $self->{version};
}

sub getParentResource {
    my ($self) = @_;

    my $parsedXml = $self->getParsedXml();

    my $parentDatasource;
    my $parentResourceName = $parsedXml->{parentResource};

    if ($parentResourceName) {
	$parentDatasource =
	    $self->{dataSources}->getDataSource($parentResourceName);
	die "Error: can't find parent resource '$parentResourceName' mentioned in $self->{dataSourceName}\n" unless $parentDatasource;
    } 
    return $parentDatasource;
}


sub getPlugin {
    my ($self) = @_;

    return $self->{plugin};
}

sub getWgetArgs {
    my ($self) = @_;

    return $self->{wgetArgs};
}

sub getWgetUrl {
    my ($self) = @_;
    
    return $self->{wgetUrl};
}

sub getManualGet {
    my ($self) = @_;
    die "Looks like there is more then one <manualGet> in resource $self->{dataSourceName}" if ref($self->{manualGet});

    return $self->{manualGet};
}

sub getManualFileOrDir {
    my ($self) = @_;

    my $manualGet = $self->getManualGet();
    my $version = $self->getVersion();
    my $resourceName = $self->getName();

    my $fileOrDir = $manualGet->{fileOrDir};
    $fileOrDir =~ s/\%RESOURCE_NAME\%/$resourceName/g;
    $fileOrDir =~ s/\%RESOURCE_VERSION\%/$version/g;
    return $fileOrDir;
}


sub getUnpacks {
    my ($self) = @_;

    return $self->{unpacks};
}

sub getGetAndUnpackOutputs {
    my ($self) = @_;

    return $self->{getAndUnpackOutput};
}

sub getPluginArgs {
    my ($self) = @_;

    my $parent = "";
    my $name = $self->getName();
    my $version =  $self->getVersion();

    my $parsedXml = $self->getParsedXml();

    my $pluginArgs = $parsedXml->{pluginArgs};

    if ($parsedXml->{parentResource}) {

      if ($pluginArgs =~ /\%(RESOURCE_\w+)\%/) {
	my $macro = $1;
	my $xmlFile = $self->{dataSources}->getXmlFile();
	die "Resource $self->{dataSourceName} in file $xmlFile has a parentResource but is using the macro \%$macro\%.  It must use \%PARENT_$macro\% instead\n";
      }
      $parent = 'PARENT_';
      $name = $self->getParentResource()->getName();
      $version = $self->getParentResource()->getVersion();
    }
    $pluginArgs =~ s/\%${parent}RESOURCE_NAME\%/$name/g;
    $pluginArgs =~ s/\%${parent}RESOURCE_VERSION\%/$version/g;
    $pluginArgs =~ s/\n+/ /g;
    return $pluginArgs;
}



1;
