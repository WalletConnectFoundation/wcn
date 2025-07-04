pub mod maintenance;
pub mod migration;
pub mod operator;
pub mod settings;
pub mod show;
pub mod signer;

#[allow(clippy::large_enum_variant)]
#[derive(clap::Subcommand, Debug)]
pub enum SubCmd {
    Signer(signer::SignerCmd),
    Show(show::ShowCmd),
    Migration(migration::MigrationCmd),
    Maintenance(maintenance::MaintenanceCmd),
    Operator(operator::OperatorCmd),
    Settings(settings::SettingsCmd),
}
