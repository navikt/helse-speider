plugins {
    alias(libs.plugins.sas.deployable)
}

sasDeployable {
    mainClass = "no.nav.helse.speider.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)
}
