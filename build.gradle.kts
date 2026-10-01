plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.speider.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)
}
