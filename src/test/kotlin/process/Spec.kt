package process

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.junit.jupiter.api.Assertions.assertEquals

class FirstSpec : Spek({
    given("A calculator") {
        val calculator = 1
        on("Adding 3 and 5") {
            val result = 2
            it("Produces 8") {
                assertEquals(8, result)
            }
        }
    }
})