package ReFlow::DataSource::DataSource;

use strict;
use Data::Dumper;

sub new {
  my ($class, $dataSourceName, $parsedXml, $dataSources) = @_;

  my $self = {};
  $self->{parsedXml} = $parsedXml;
  $self->{dataSourceName} = $dataSourceName;
  $self->{dataSources} = $dataSources;

  bless($self,$class);

  return $self;
}

sub getName {
    my ($self) = @_;

    return $self->{dataSourceName};
}

sub getVersion {
    my ($self) = @_;

    return $self->{parsedXml}->{version};
}

sub getDisplayName {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{displayName};
}

sub getOrganisms {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{organisms};
}

sub getParentResource {
    my ($self) = @_;

    my $parentResource= $self->{parsedXml}->{parentResource};

    return $self->{dataSources}->getDataSource($parentResource) if $parentResource;
}

sub getProject {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{project};
}

sub getCategory {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{category};
}

sub getPlugin {
    my ($self) = @_;

    return $self->{parsedXml}->{plugin};
}

sub getWgetArgs {
    my ($self) = @_;

    return $self->{parsedXml}->{wgetArgs}->{content};
}

sub getWgetUrl {
    my ($self) = @_;

    return $self->{parsedXml}->{wgetArgs}->{url};
}

sub getManualGet {
    my ($self) = @_;

    return $self->{parsedXml}->{manualGet};
}

sub getManualFileOrDir {
    my ($self) = @_;

    my $fileOrDir = $self->{parsedXml}->{manualGet}->{fileOrDir};
    $fileOrDir =~ s/\%RESOURCE_NAME\%/$self->{dataSourceName}/g;
    $fileOrDir =~ s/\%RESOURCE_VERSION\%/$self->{parsedXml}->{version}/g;
    return $fileOrDir;
}

sub getContact {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{contact};
}

sub getEmail {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{email};
}

sub getInstitution {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{institution};
}

sub getUnpacks {
    my ($self) = @_;

    return $self->{parsedXml}->{unpack} || [];
}

sub getGetAndUnpackOutputs {
    my ($self) = @_;

    return $self->{parsedXml}->{getAndUnpackOutput} || [];
}

sub getPluginArgs {
    my ($self) = @_;

    my $parent = "";
    my $name = $self->{dataSourceName};
    my $version =  $self->{parsedXml}->{version};
    my $pluginArgs = $self->{parsedXml}->{pluginArgs};
    if ($self->{parsedXml}->{parentResource}) {

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
    return $pluginArgs;
}

sub getDescription {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{description};
}

# returns reference to an array of hash references with keys:
#   pmid, doi, citation
sub getPublications {
    my ($self) = @_;

    if (!$self->{publications}) {
	foreach my $publication (@{$self->{parsedXml}->{info}->{publication}}) {
	    my $pubmedId = $publication->{pmid};
	    if ($pubmedId) {
		$publication->{citation} = `pubmedIdToCitation $pubmedId`;
		die "failed calling 'pubmedIdToCitation $pubmedId'" if $? >> 8;
	    }
	}
	$self->{publications} = $self->{parsedXml}->{info}->{publication};
    }
    return $self->{publications};
}

# returns reference to an array of hash references with keys:
# record_class, type, name
sub getWdkReferences {
    my ($self) = @_;

    return $self->{parsedXml}->{info}->{wdkReference};
}


1;
