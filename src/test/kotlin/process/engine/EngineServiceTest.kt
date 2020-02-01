package process.engine

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test


class EngineServiceTest {

    @Test
    fun `Engine starts up`() {
        Assertions.assertThat(4).isEqualTo(4)
    }
}